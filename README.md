# Setup instructions
1. Create a schema(database) in MySQL instance
2. Create ".env" file according to ".env_example" and fill in all required information
3. Run "python -m venv testenv"
4. Run "source testenv/bin/activate"
5. Run "pip install -r requirements.txt"

# How to run the pipeline
1. Main program
    - Run "source testenv/bin/activate"
    - Run "python pipeline.py"
2. Testing
    - Run "source testenv/bin/activate"
    - Run "python pipeline_test.py"

# Dependencies and requirements
- Python==3.14.2
- python-dotenv==1.2.*
- pandas==2.3.*
- pycountry==24.6.*
- translate=3.8.*
- mysql-connector-python==9.5.*