import openpyxl
import xlrd
import pydicom
from PyPDF2 import PdfReader
from mutagen.mp3 import MP3
from PIL import Image
from PIL.ExifTags import TAGS
import os, json
from pathlib import Path

def get_excel_metadata(filepath):
    ext = filepath.lower().split('.')[-1]
    if ext == 'xlsx':
        wb = openpyxl.load_workbook(filepath, read_only=True)
        props = wb.properties
        return {
            "title": props.title,
            "author": props.creator,
            "created": props.created,
            "modified": props.modified
        }
    elif ext == 'xls':
        book = xlrd.open_workbook(filepath)
        return {
            "sheet_names": book.sheet_names(),
            "num_sheets": book.nsheets
        }


def get_pdf_metadata(filepath):
    reader = PdfReader(filepath)
    info = reader.metadata
    return dict(info)


def get_mp3_metadata(filepath):
    audio = MP3(filepath)
    return {k: str(v) for k, v in audio.items()}


def get_image_metadata(filepath):
    img = Image.open(filepath)
    info = img.info
    exif_data = img._getexif()
    exif = {}
    if exif_data:
        for tag, value in exif_data.items():
            name = TAGS.get(tag, tag)
            exif[name] = value
    return {**info, **exif}


def get_text_metadata(filepath):
    stat = os.stat(filepath)
    return {
        "modified": stat.st_mtime,
        "created": stat.st_ctime,
    }

def get_dicom_metadata(filepath):
    ds = pydicom.dcmread(filepath, stop_before_pixels=True)
    return {elem.keyword: elem.value for elem in ds if elem.keyword}

def extract_all_metadata(filepath):
    ext = Path(filepath).suffix.lower()
    try:
        if ext in ['.xlsx', '.xls']:
            data = get_excel_metadata(filepath)
        elif ext == '.pdf':
            data = get_pdf_metadata(filepath)
        elif ext == '.mp3':
            data = get_mp3_metadata(filepath)
        elif ext in ['.jpg', '.jpeg', '.png', '.tiff', '.tif']:
            data = get_image_metadata(filepath)
        elif ext in ['.txt', '.md']:
            data = get_text_metadata(filepath)
        elif ext == '.dcm':
            data = get_dicom_metadata(filepath)
        else:
            return {"error": "Unsupported file type"}

        return data if data else {"warning": "No metadata found"}

    except Exception as e:
        return {"error": str(e)}


def process_folder_to_metadata_json(folder_path):
    results = {}
    for file in os.listdir(folder_path):
        full_path = os.path.join(folder_path, file)
        if os.path.isfile(full_path):
            print(f"Processing: {file}")
            metadata = extract_all_metadata(full_path)

            if isinstance(metadata, dict) and (
                not metadata or 
                ("warning" in metadata and metadata["warning"] == "No metadata found")
            ):
                print(f"  No metadata for: {file}")
                results[file] = {"info": "No metadata found"}
            else:
                results[file] = metadata
    return results

def get_first_1000_filenames(folder_path):
    file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    return file_names[:1000]

