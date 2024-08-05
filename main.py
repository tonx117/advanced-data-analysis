import mysql.connector
import pandas as pd
import sys
import matplotlib.pyplot as plt

# Esta función se encarga de conectarse a la base de datos MySQL
def connect_db():
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="companydata",
            port="3208"  
        )
    except mysql.connector.Error as e:
        print("No se pudo conectar:", e)
        sys.exit(1)
    print("Conexión correcta")
    return db

#  Esta función crea la tabla EmployeePerformance en la base de datos
def create_db(db):
    cursor = db.cursor()
    sql_delete = "DROP TABLE IF EXISTS EmployeePerformance"
    cursor.execute(sql_delete)
    db.commit()
    sql_create = """CREATE TABLE IF NOT EXISTS EmployeePerformance(
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    department VARCHAR(255),
    performance_score DECIMAL(5,2),
    years_with_company INT,
    salary DECIMAL(10,2))"""
    cursor.execute(sql_create)
    db.commit()
    print("Tabla creada correctamente")
    
create_db(connect_db())

# Esta función importa datos de un archivo CSV a la base de datos
def import_csv_to_db(csv_file, db):
    cursor = db.cursor()
    df = pd.read_csv(csv_file)  # Lee el archivo CSV en un DataFrame de pandas
    print("Columnas en el CSV:", df.columns)  # Imprime las columnas del CSV para verificación
    for i, row in df.iterrows():  # Itera sobre cada fila del DataFrame
        # Extrae los valores de las columnas en el orden especificado
        values = tuple(row[col] for col in ['employee_id', 'department', 'performance_score', 'years_with_company', 'salary'])
        sql_insert = """INSERT INTO EmployeePerformance (employee_id, department, performance_score, years_with_company, salary)
                        VALUES (%s, %s, %s, %s, %s)"""
        cursor.execute(sql_insert, values)  # Ejecuta la inserción en la base de datos
    db.commit()  # Confirma los cambios en la base de datos
    print("Datos importados correctamente")

# Conecta a la base de datos
db = connect_db()
# Importa los datos del CSV a la base de datos
import_csv_to_db('./MOCK_DATA.csv', db)

# Lee los datos de la tabla EmployeePerformance en un DataFrame de pandas
df = pd.read_sql_query("SELECT * FROM EmployeePerformance", db)

# Obtiene los nombres únicos de los departamentos
departamentos = df['department'].unique()

# Crea un diccionario para almacenar estadísticas por departamento
estadísticas = {}

# Calcula estadísticas para cada departamento
for departamento in departamentos:
    df_departamento = df[df['department'] == departamento]  # Filtra los datos por departamento
    
    # Calcula la media, mediana y desviación estándar del performance_score
    performance_score_media = df_departamento['performance_score'].mean()
    performance_score_mediana = df_departamento['performance_score'].median()
    performance_score_desviacion = df_departamento['performance_score'].std()

    # Calcula la media, mediana y desviación estándar del salario
    salary_media = df_departamento['salary'].mean()
    salary_mediana = df_departamento['salary'].median()
    salary_desviacion = df_departamento['salary'].std()

    # Cuenta el número de empleados en el departamento
    num_empleados = df_departamento.shape[0]

    # Calcula la correlación entre años con la empresa y performance_score
    correlacion_years_performance = df_departamento['years_with_company'].corr(df_departamento['performance_score'])

    # Calcula la correlación entre salario y performance_score
    correlacion_salary_performance = df_departamento['salary'].corr(df_departamento['performance_score'])

    # Almacena las estadísticas en el diccionario
    estadísticas[departamento] = {
        'performance_score_media': performance_score_media,
        'performance_score_mediana': performance_score_mediana,
        'performance_score_desviacion': performance_score_desviacion,
        'salary_media': salary_media,
        'salary_mediana': salary_mediana,
        'salary_desviacion': salary_desviacion,
        'num_empleados': num_empleados,
        'correlacion_years_performance': correlacion_years_performance,
        'correlacion_salary_performance': correlacion_salary_performance
    }

# Imprime las estadísticas por departamento
for departamento, estadística in estadísticas.items():
    print(f"Departamento: {departamento}")
    print(f"Media del performance_score: {estadística['performance_score_media']}")
    print(f"Mediana del performance_score: {estadística['performance_score_mediana']}")
    print(f"Desviación estándar del performance_score: {estadística['performance_score_desviacion']}")
    print(f"Media del salary: {estadística['salary_media']}")
    print(f"Mediana del salary: {estadística['salary_mediana']}")
    print(f"Desviación estándar del salary: {estadística['salary_desviacion']}")
    print(f"Número total de empleados: {estadística['num_empleados']}")
    print(f"Correlación entre years_with_company y performance_score: {estadística['correlacion_years_performance']}")
    print(f"Correlación entre salary y performance_score: {estadística['correlacion_salary_performance']}")
    print()

# Crea un histograma del performance_score por departamento
for departamento in departamentos:
    df_departamento = df[df['department'] == departamento]
    plt.hist(df_departamento['performance_score'], bins=10, alpha=0.5, label=departamento)
plt.xlabel('Performance Score')
plt.ylabel('Frecuencia')
plt.title('Histograma del Performance Score por Departamento')
plt.legend()
plt.show()

# Crea un gráfico de dispersión de years_with_company vs. performance_score
plt.scatter(df['years_with_company'], df['performance_score'])
plt.xlabel('Años con la empresa')
plt.ylabel('Performance Score')
plt.title('Gráfico de dispersión de years_with_company vs. performance_score')
plt.show()

# Crea un gráfico de dispersión de salary vs. performance_score
plt.scatter(df['salary'], df['performance_score'])
plt.xlabel('Salario')
plt.ylabel('Performance Score')
plt.title('Gráfico de dispersión de salary vs. performance_score')
plt.show()
