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

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

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
            
    def get_tables(self, conn):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [table[0] for table in self.cursor.fetchall()]

    def get_columns(self, conn, table_name):
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        return [column[1] for column in self.cursor.fetchall()]

if __name__ == "__main__":
    db_manager = DatabaseManager()
    
    sqlite_files = glob.glob(os.path.join(db_manager.dir_path, '*.sqlite'))
    
    if not sqlite_files:
        logging.error("Nenhum arquivo .sqlite encontrado no diretório.")
        sys.exit(1)
        
    logging.info("\nArquivos database encontrados:")
    for idx, file in enumerate(sqlite_files):
        logging.info(f"{idx + 1}. {os.path.basename(file)}")
        
    while True:
        try:
            choice = int(input("\nSelecione o numero do aquivo para processar: "))
            if 1<= choice <= len(sqlite_files):
                db = sqlite_files[choice - 1]
                break
            logging.info(f"Escolha inválida. Por favor, selecione um número entre 1 e {len(sqlite_files)}.")
        except ValueError:
            logging.info("Entrada inválida. Por favor, insira um número válido.")
            
    conn = sqlite3.connect(db_manager.db_path)
    tables = db_manager.get_tables(conn)
    logging.info("\nTabelas encontradas no banco de dados: ")
    for idx, table in enumerate(tables):
        logging.info(f"{idx + 1}. {table}")
        
    while True:
        try:
            choice = int(input("\nSelecione o numero da tabela para processar: "))
            if 1 <= choice <= len(tables):
                db_table = tables[choice - 1]
                break
            logging.info(f"Escolha inválida. Por favor, selecione um número entre 1 e {len(tables)}.")
        except ValueError:
            logging.info("Entrada inválida. Por favor, insira um número válido.")
    
    columns = db_manager.get_columns(conn, db_table)
    logging.info("\nColunas encontradas na tabela selecionada: ")
    for idx, column in enumerate(columns):
        logging.info(f"{idx + 1}. {column}")
    
    while True:
        try:
            choice = int(input("\nSelecione o numero da coluna de descrição para processar: "))
            if 1 <= choice <= len(columns):
                description_column = columns[choice - 1]
                break
            logging.info(f"Escolha inválida. Por favor, selecione um número entre 1 e {len(columns)}.")
        except ValueError:
            logging.info("Entrada inválida. Por favor, insira um número válido.")
    conn.close()

    logging.info(f"\nIniciando o processamento do arquivo '{os.path.basename(db)}', tabela '{db_table}', coluna '{description_column}'.\n")
    logging.info(f"Tabela: {db_table}, Coluna: {description_column}\n")
    
    db_manager.process_database(table_name=db_table, description_column=description_column)