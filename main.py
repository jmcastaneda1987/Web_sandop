from flask_bootstrap import Bootstrap5
from flask import Flask, render_template, request, send_file, redirect,url_for
import pandas as pd
from io import BytesIO
from pipeline import Reporte
from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from wtforms import FileField,SubmitField,SelectField
from wtforms.validators import DataRequired


app = Flask(__name__)
app.config['SECRET_KEY'] = '12345678jhgf'
Bootstrap5(app)
csrf = CSRFProtect(app)
final_report = Reporte()

class UploadForm(FlaskForm):
    rolling_file = FileField('Cargar Rundown Rolling', validators=[DataRequired()])
    complementaria_file = FileField('Cargar Complementaria', validators=[DataRequired()])
    clientes_file = FileField('Cargar Informacion de clientes', validators=[DataRequired()])
    submit = SubmitField("Cargar!")

class CalcForm(FlaskForm):
    opciones = [('01', 'Enero'), ('02', 'Febrero'), ('04', 'Marzo')]
    dropdown = SelectField('Selecciona una opción', choices=opciones)
    submit1 = SubmitField("Calcular")


@app.route('/', methods=['GET', 'POST'])
def home():
    form = UploadForm()
    form1 = CalcForm()
    if form.submit.data and form.validate(): # notice the order
        archivo_bytes = form.rolling_file.data.read()
        final_report.data_rolling = pd.read_excel(BytesIO(archivo_bytes))
        archivo_bytes = form.complementaria_file.data.read()
        final_report.data_complementaria = pd.read_excel(BytesIO(archivo_bytes),sheet_name='Hoja1')
        archivo_bytes = form.clientes_file.data.read()
        final_report.data_clientes = pd.read_excel(BytesIO(archivo_bytes),sheet_name='DATA')
        return redirect(url_for("home"))
    if form1.submit1.data and form1.validate(): # notice the order
        final_report.mes_de_corte = int(form1.dropdown.data)
        final_report.calcular()
        data = final_report.resultado.values.tolist()
        headings = final_report.resultado.columns.values
        return render_template("index.html", form=form, form1=form1, headings=headings, data=data)
    return render_template("index.html", form=form, form1=form1)


@app.route('/descargar')
def download():
    resultado_bytes = BytesIO()
    final_report.resultado.to_excel(resultado_bytes, index=False)
    resultado_bytes.seek(0)
    return send_file(resultado_bytes, as_attachment=True, download_name='resultado.xlsx')


@app.route('/prueba')
def prueba():
    return render_template("mitabladeprueba.html")


if __name__ == "__main__":
    app.run(debug=True, port=8080)