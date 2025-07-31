import os
import openpyxl
import xlrd
import pydicom
from PyPDF2 import PdfReader
from mutagen.mp3 import MP3
from PIL import Image

def check_file_openability(directory):
    """
    Check if files in a directory are openable by their type-specific library.
    Returns counts and percentages of openable and not openable files.
    """
    from pathlib import Path

    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    sample = files[:min(10, len(files))]
    openable_count = 0
    not_openable_count = 0
    openable_files = []
    not_openable_files = []

    for file in sample:
        file_path = os.path.join(directory, file)
        ext = Path(file).suffix.lower()
        try:
            if ext in ['.xlsx']:
                openpyxl.load_workbook(file_path, read_only=True)
            elif ext in ['.xls']:
                xlrd.open_workbook(file_path)
            elif ext == '.pdf':
                PdfReader(file_path)
            elif ext == '.mp3':
                MP3(file_path)
            elif ext in ['.jpg', '.jpeg', '.png', '.tiff', '.tif']:
                with Image.open(file_path) as img:
                    img.verify()  # verify() is fast and doesn't decode the image data
            elif ext in ['.txt', '.md']:
                with open(file_path, 'r') as f:
                    f.read(1024)  # Try reading a small chunk
            elif ext == '.dcm':
                pydicom.dcmread(file_path, stop_before_pixels=True)
            else:
                # For unsupported types, just try opening as binary
                with open(file_path, 'rb') as f:
                    f.read(1024)
            openable_count += 1
            openable_files.append(file)
        except Exception:
            not_openable_count += 1
            not_openable_files.append(file)

    total_checked = len(sample)
    not_openable_percentage = (not_openable_count / total_checked) * 100 if total_checked else 0
    openable_percentage = (openable_count / total_checked) * 100 if total_checked else 0

    return {
        "file_openable_count": openable_count,
        "file_openable_percentage": openable_percentage,
        "file_not_openable_count": not_openable_count,
        "file_not_openable_percentage": not_openable_percentage,
        "openable_files": openable_files,
        "not_openable_files": not_openable_files
    }