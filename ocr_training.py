import sqlite3
import logging
from typing import Set
from unidecode import unidecode
import os
import sys
import glob

class DatabaseManager:
    def __init__(self):
        if getattr(sys, 'frozen', False):
            self.dir_path = os.path.dirname(sys.executable)
        else:
            self.dir_path = os.path.dirname(os.path.abspath(__file__))
        
        db_path = glob.glob(os.path.join(self.dir_path, '*.sqlite'))
        self.db_path = db_path[0] if db_path else None
        
        self.log_dir = os.path.join(self.dir_path, 'ocr_training.log')
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_dir),
                logging.StreamHandler()
            ]
        )
    
    def generate_variations(self, text: str) -> Set[str]:
        variations = {text}
        
        ocr_substitutions = {
            'O': '0', '0': 'O',
            'I': '1', '1': 'I',
            'Z': '2', '2': 'Z',
            'E': '3', '3': 'E',
            'A': '4', '4': 'A',
            'S': '5', '5': 'S'
        }
        
        variations.add(text.upper())
        variations.add(text.lower())
        variations.add(unidecode(text))
        variations.add(text.replace(' ', ''))
        
        for char in text:
            if char in ocr_substitutions:
                new_text = text.replace(char, ocr_substitutions[char])
                variations.add(new_text)
                variations.add(new_text.upper())
                variations.add(new_text.lower())
                
        return variations

    def process_database(self, table_name: str, description_column: str):
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name}_variations (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            original_text TEXT,
                            variation TEXT,
                            UNIQUE(original_text, variation)
                        )""")
            cur.execute(f"SELECT DISTINCT {description_column} FROM {table_name}")
            
            items = cur.fetchall()
            
            logging.info(f"Encontrados {len(items)} itens na tabela {table_name}.")
            
            for i, (item,) in enumerate(items):
                if not item:
                    continue
                
                variations = self.generate_variations(item)
                
                for variation in variations:
                    try:
                        cur.execute(f"""
                                    INSERT OR IGNORE INTO {table_name}_variations
                                    (original_text, variation)
                                    VALUES (?, ?)
                                    """, (item, variation))
                    except sqlite3.IntegrityError as e:
                        logging.error(f"Erro ao inserir variação '{variation}' para '{item}': {e}")
                
                if i % 100 == 0:
                    logging.info(f"Processados {i} itens.")
                    conn.commit()
                    
            conn.commit()
            
            cur.execute(f"SELECT COUNT(*) FROM {table_name}_variations")
            total_variations = cur.fetchone()[0]
            logging.info(f"Total de variações geradas e armazenadas: {total_variations}.")
            
        except sqlite3.Error as e:
            logging.error(f"Erro ao processar a tabela {table_name}: {e}")
        finally:
            conn.close()
            
if __name__ == "__main__":
    db_manager = DatabaseManager()
    
    logging.info(f"Using database: {db_manager.db_path}")
    table_name = input("Enter the table name to process: ")
    description_column = input("Enter the description column name: ")
    db_manager.process_database(table_name=table_name, description_column=description_column)