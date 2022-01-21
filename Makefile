create_tables:
	python table_creation/create_tables.py

etl:
	python table_creation/create_tables.py
	python etl.py
