import textract
import pandas as pd
import sys
import uuid
import hashlib
import multiprocessing
import sqlite3
import os

input_dir = sys.argv[1]

def traverse_dir(dir):
	allowed_extensions = ('.doc', '.docx', '.epub', '.htm', '.html', '.pdf', '.pptx', '.ps', '.rtf', '.txt')
	for x,y,z in os.walk(dir):
		for file in z:
			if file.endswith(allowed_extensions):
				yield os.path.join(x,file)

def text_extract(file):
	text = textract.process(file)
	return text

def tokenize(text):
	tokens = text.split()
	return tokens

def create_metadata_file(rows):
	metadata_table = pd.DataFrame(rows, columns=["uuid", "path", "filename", "tokens", "md5"])
	metadata_table.to_excel('metadata.xlsx')

def get_md5_hash(file):
	with open(file, 'rb') as f:
		file_stream = f.read()
	return hashlib.md5(file_stream).hexdigest()

def create_db():
	with sqlite3.connect('ft_raw.db') as con:
		cur = con.cursor()
		cur.execute("CREATE TABLE ft_raw (uuid_str text, file_path text, basename text, number_tokens int, md5_hash text, tokens text);")

def insert_to_db(row)::
	with sqlite3.connect('ft_raw.db') as con:
		cur = con.cursor()
		cur.execute('INSERT INTO ft_raw VALUES(?,?,?,?,?,?);', row)
		con.commit()

def parallel(file):
	try:
		text = text_extract(file)
		tokens = tokenize(text)
		uuid_str = str(uuid.uuid4())
		md5 = get_md5_hash(file)
		row = (uuid_str, file, os.path.basename(file), len(tokens), md5, ' '.join(tokens))
	except:
		row = None
	return row

def main():
	pool = multiprocessing.Pool(5)

	for row in pool.imap_unordered(parallel, traverse_dir(input_dir)):
		if row:
			print(row)

	create_metadata_file(rows)

if __name__ == "__main__":
	main()

