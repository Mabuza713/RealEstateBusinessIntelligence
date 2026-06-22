"""
Potok pomocniczy (Utility DAG) służący do tworzenia kopii zapasowych surowych plików CSV
oraz cofania (Rollback) ostatnio wczytanego miesiąca.
Umożliwia łatwe ponowne przetestowanie mechanizmów przyrostowego zasilania.
"""

from datetime import datetime
import shutil
import pandas as pd
from pathlib import Path
import glob

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

def _backup_and_remove_last_month(**context) -> None:
    """
    Wykonuje kopie zapasowe plików all_apartments*.csv z data/raw/
    oznaczając kopie znacznikiem czasu, a następnie usuwa z plików źródłowych
    wszystkie wiersze z najnowszego miesiąca w kolumnie source_date.
    """
    
    # 1. Określenie ścieżek do plików raw i backup
    root_path = Path("/opt/airflow") if Path("/opt/airflow/dags").exists() else Path(__file__).resolve().parents[1]
    data_dir = root_path / "data" / "raw"
    backup_dir = data_dir / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 2. Wyszukanie wszystkich plików z ofertami mieszkań w data/raw/
    csv_pattern = str(data_dir / "all_apartments*.csv")
    filepaths = [Path(p) for p in glob.glob(csv_pattern)]
    
    if not filepaths:
        print("Nie znaleziono żadnych plików matching all_apartments*.csv w data/raw.")
        return
        
    print(f"Znaleziono następujące pliki do przetworzenia: {[p.name for p in filepaths]}")
    
    # 3. Kopiowanie bezpieczeństwa i filtrowanie rekordów
    for filepath in filepaths:
        if filepath.exists() and filepath.stat().st_size > 0:
            # Tworzenie kompletnej kopii zapasowej (całego pliku) przed jakąkolwiek modyfikacją (shutil.copy2 zachowuje metadane)
            backup_path = backup_dir / f"{filepath.stem}_{timestamp}{filepath.suffix}"
            shutil.copy2(filepath, backup_path)
            print(f"Utworzono kopię zapasową całego pliku {filepath.name} w: {backup_path}")
            
            try:
                # Usunięcie wierszy z ostatniego miesiąca z pliku CSV na poziomie Pandas
                df = pd.read_csv(filepath)
                if 'source_date' in df.columns:
                    # Wyciągnięcie unikalnych miesięcy (pierwsze 7 znaków formatu YYYY-MM-DD)
                    months = df['source_date'].dropna().astype(str).str[:7].unique()
                    if len(months) > 0:
                        # Identyfikacja najnowszego miesiąca
                        latest_month = max(months)
                        initial_count = len(df)
                        
                        # Odfiltrowanie (usunięcie) wierszy należących do najnowszego miesiąca
                        df_filtered = df[~df['source_date'].astype(str).str.startswith(latest_month)]
                        removed_count = initial_count - len(df_filtered)
                        
                        if removed_count > 0:
                            # Zapisanie zmodyfikowanego pliku z powrotem na dysk
                            df_filtered.to_csv(filepath, index=False, encoding="utf-8-sig")
                            print(f"Plik {filepath.name}: Usunięto {removed_count} wierszy dla najnowszego miesiąca {latest_month}")
                        else:
                            print(f"Plik {filepath.name}: Brak wierszy do usunięcia dla najnowszego miesiąca {latest_month}")
                    else:
                        print(f"Plik {filepath.name}: Brak poprawnych dat w kolumnie 'source_date'. Pomijam filtrowanie.")
                else:
                    print(f"Plik {filepath.name} nie zawiera kolumny 'source_date'! Pomijam filtrowanie.")
            except Exception as e:
                print(f"Błąd podczas przetwarzania pliku {filepath.name}: {e}")
        else:
            print(f"Plik {filepath.name} nie istnieje lub jest pusty. Pomijam.")


# Definicja DAG-a pomocniczego: schedule=None umożliwia tylko ręczne uruchamianie przez UI
with DAG(
    dag_id="Backup_and_Remove_Last_Month_DAG",
    default_args={"owner": "Mabuza", "retries": 0},
    description="Tworzy kopię zapasową all_apartments i usuwa ostatni miesiąc wyłącznie z plików CSV",
    schedule=None,
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=["utility", "testing"],
) as dag:

    backup_and_clean_task = PythonOperator(
        task_id="backup_and_remove_last_month",
        python_callable=_backup_and_remove_last_month
    )

    backup_and_clean_task
