import io
from flask import Flask, render_template, request, send_file

from statement_parser.extractors.tabula import *

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        bank = request.form['bank']
        statement_type = request.form['statement_type']
        pdf_files = request.files.getlist('pdf_files')

        match bank, statement_type:
            case 'DBS', 'CASA':
                extractor = DbsCasa()
            case 'DBS', 'Cards':
                extractor = DbsCard()
            case 'OCBC', 'CASA':
                extractor = OcbcCasa()
            case 'OCBC', 'Cards':
                extractor = OcbcCard()
            case 'UOB', 'CASA':
                extractor = UobCasa()
            case 'UOB', 'Cards':
                extractor = UobCard()

        df = extractor.extract_files(pdf_files)

        # Save DataFrame to a CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Send the CSV file as a downloadable attachment
        return send_file(
            io.BytesIO(csv_buffer.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name='bank_statements.csv'
        )

    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
