import pandas as pd
import shutil
import os

def process_file(filepath):
    """
    Processes the given file by splitting its content into sections and saving each section as a separate CSV file.
    If the file is encoded in UTF-16 without BOM, it will be copied directly to the temp directory.
    
    Parameters:
        filepath (str): The path to the file to be processed.
    """
    temp_dir = 'temp'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    if os.path.basename(filepath).startswith(('Stories', 'Posts')):
        filename = os.path.join(temp_dir, os.path.basename(filepath))
        shutil.copy(filepath, filename)
        return 

    try:
        with open(filepath, 'r', encoding='utf-16') as file:
            lines = file.readlines()

        if lines[0].startswith('sep=,'):
            lines = lines[1:]

        content_parts = []
        current_part = []
        
        for line in lines:
            if line.strip() == "":
                if current_part:
                    content_parts.append(current_part)
                current_part = []
            else:
                current_part.append(line)
        
        if current_part:
            content_parts.append(current_part)
        
        for part in content_parts:
            header = part[0].strip().replace('"', '').replace(' ', '_')
            part = part[1:]
            filename = os.path.join(temp_dir, f"{header}.csv")
            
            with open(filename, 'w', encoding='utf-8', newline='') as outfile:
                outfile.writelines(part)
            
            print(f"File saved: {filename}")

    except Exception as e:
        print(f"Error processing the file: {e}")

def process_extraction_files(extraction_dir):
    """
    Process the extraction files in the given directory.
    
    Parameters:
        extraction_dir (str): The path to the directory containing the extraction files.
    """
    extraction_files = os.listdir(extraction_dir)
    for file in extraction_files:
        process_file(os.path.join(extraction_dir, file))

    return