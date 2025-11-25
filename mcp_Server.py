import os
import sqlite3
from pathlib import Path
from fastmcp import FastMCP

# --- CONFIGURATION ---
# The folder the AI is allowed to access. 
# Defaults to a "workspace" folder in the same directory as this script.
TARGET_FOLDER = Path("./workspace").resolve()
TARGET_FOLDER.mkdir(parents=True, exist_ok=True)

# Initialize Server
mcp = FastMCP("Local File & DB Server", host="0.0.0.0", port=8000)

# --- HELPER FUNCTIONS ---

def _get_safe_path(filename: str) -> Path:
    """Security check to prevent directory traversal."""
    safe_name = os.path.basename(filename)
    full_path = (TARGET_FOLDER / safe_name).resolve()
    if not str(full_path).startswith(str(TARGET_FOLDER)):
        raise ValueError(f"Access denied: {filename} is outside the allowed directory.")
    return full_path

def _read_pdf(path: Path) -> str:
    """Helper to extract text from PDF."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except ImportError:
        return "Error: 'pypdf' not installed."
    except Exception as e:
        return f"Error reading PDF: {str(e)}"

def _read_docx(path: Path) -> str:
    """Helper to extract text from Word Docs."""
    try:
        import docx
        doc = docx.Document(path)
        return "\n".join([para.text for para in doc.paragraphs])
    except ImportError:
        return "Error: 'python-docx' not installed."
    except Exception as e:
        return f"Error reading DOCX: {str(e)}"

# --- FILE SYSTEM TOOLS ---

@mcp.tool()
def list_files() -> str:
    """Lists all files currently in the workspace."""
    try:
        files = [f.name for f in TARGET_FOLDER.iterdir() if f.is_file()]
        if not files:
            return "The workspace is empty."
        return "Files available:\n" + "\n".join(f"- {f}" for f in files)
    except Exception as e:
        return f"Error listing files: {str(e)}"

@mcp.tool()
def read_file(filename: str) -> str:
    """
    Reads a file (txt, md, py, json, pdf, docx).
    For .db/.sqlite files, use 'inspect_sql_db' instead.
    """
    try:
        file_path = _get_safe_path(filename)
        if not file_path.exists():
            return f"Error: File '{filename}' not found."

        suffix = file_path.suffix.lower()
        if suffix == ".pdf":
            return _read_pdf(file_path)
        elif suffix == ".docx":
            return _read_docx(file_path)
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except UnicodeDecodeError:
            return "Error: Binary file detected. Use 'inspect_sql_db' for databases."
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def write_to_file(filename: str, content: str, mode: str = "w") -> str:
    """
    Creates or Edits a text-based file.
    mode: 'w' (overwrite), 'a' (append).
    """
    try:
        file_path = _get_safe_path(filename)
        if file_path.exists() and file_path.suffix.lower() in ['.pdf', '.docx', '.db', '.sqlite']:
            return "Error: Cannot write text directly to binary/database files."
        
        write_mode = "a" if mode == "a" else "w"
        with open(file_path, write_mode, encoding="utf-8") as f:
            f.write(content)
        return f"✅ Successfully wrote to {filename}"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def delete_file(filename: str) -> str:
    """
    Permanently deletes a file from the workspace.
    Works for any file type including databases.
    """
    try:
        file_path = _get_safe_path(filename)
        if not file_path.exists():
            return f"Error: File '{filename}' not found."
        
        os.remove(file_path)
        return f"✅ Successfully deleted {filename}"
    except Exception as e:
        return f"Error deleting file: {str(e)}"

# --- DATABASE TOOLS ---

@mcp.tool()
def create_sql_db(filename: str) -> str:
    """
    Creates a new empty SQLite database file.
    Filename must end in .db or .sqlite
    """
    if not (filename.endswith(".db") or filename.endswith(".sqlite")):
        return "Error: Database filename must end with .db or .sqlite"

    try:
        file_path = _get_safe_path(filename)
        if file_path.exists():
            return f"Error: File '{filename}' already exists."
            
        # Connecting to a non-existent file creates it
        conn = sqlite3.connect(file_path)
        conn.close()
        return f"✅ Created new database: {filename}"
    except Exception as e:
        return f"Error creating database: {str(e)}"

@mcp.tool()
def inspect_sql_db(filename: str) -> str:
    """Inspects a SQLite database schema (tables and columns)."""
    try:
        file_path = _get_safe_path(filename)
        if not file_path.exists():
            return "Error: Database file not found."
            
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            conn.close()
            return "Database is empty."
            
        report = f"Schema for {filename}:\n"
        for table in tables:
            t_name = table[0]
            report += f"\nTable: {t_name}\n" + "-" * 20 + "\n"
            cursor.execute(f"PRAGMA table_info({t_name});")
            for col in cursor.fetchall():
                report += f"  - {col[1]} ({col[2]})\n"
                
        conn.close()
        return report
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def run_sql_query(filename: str, query: str) -> str:
    """
    Executes a SQL query (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE).
    Auto-commits changes.
    """
    try:
        file_path = _get_safe_path(filename)
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            conn.commit()
            
            if cursor.description:
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description]
                conn.close()
                if not rows: return "Query returned 0 results."
                return str([dict(zip(cols, row)) for row in rows])
            else:
                changes = conn.total_changes
                conn.close()
                return f"✅ Success. Rows affected: {changes}"
                
        except Exception as e:
            conn.close()
            return f"Query Execution Error: {str(e)}"
    except Exception as e:
        return f"System Error: {str(e)}"

if __name__ == "__main__":
    print(f"📂 Serving files from: {TARGET_FOLDER}")
    mcp.run(transport="streamable-http")