import io
from flask import Flask, jsonify, render_template, request, send_file
from flask_cors import CORS
from pydantic import BaseModel

from utils import _process_files, _save_base64_to_tempfile

app = Flask(__name__)
CORS(app)  # Allows all origins


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        bank = request.form['bank']
        statement_type = request.form['statement_type']
        pdf_files = request.files.getlist('pdf_files')
        download_name = request.form['download_file_name']
        df = _process_files(bank, statement_type, pdf_files)

        # Save DataFrame to a CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Send the CSV file as a downloadable attachment
        return send_file(
            io.BytesIO(csv_buffer.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=download_name,
        )

    return render_template('index.html')


class ExtractPayload(BaseModel):
    bank: str
    statement_type: str
    document: str  # base64 string


@app.route('/extract', methods=['POST'])
def extract():
    try:
        data = request.get_json()
        bank = data.get('bank')
        statement_type = data.get('statement_type')
        documents = data.get('documents')  # Expecting a list of base64 strings

        if not all([bank, statement_type, documents]):
            return jsonify({"error": "Missing required fields"}), 400

        # process documents
        filepaths = [_save_base64_to_tempfile(d) for d in documents]
        df = _process_files(bank, statement_type, filepaths)
        df['id'] = [i for i in range(df.shape[0])]
        
        # TODO: may need to implement renaming for different banks
        df = df[['id', 'TransactionDate', 'Amount', 'Description']]

        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        # Send as file response
        return send_file(
            io.BytesIO(csv_buffer.getvalue().encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name='extracted_data.csv'
        )
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
