import mysql.connector
import pandas as pd
import sys
import matplotlib.pyplot as plt

class DatabaseConnection:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            print("Conexión correcta")
        except mysql.connector.Error as e:
            print("No se pudo conectar:", e)
            sys.exit(1)
        return self.connection

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Conexión cerrada")

class EmployeePerformanceDatabase:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def create_table(self):
        cursor = self.db_connection.cursor()
        sql_delete = "DROP TABLE IF EXISTS EmployeePerformance"
        cursor.execute(sql_delete)
        self.db_connection.commit()
        sql_create = """CREATE TABLE IF NOT EXISTS EmployeePerformance(
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        employee_id INT,
                        department VARCHAR(255),
                        performance_score DECIMAL(5,2),
                        years_with_company INT,
                        salary DECIMAL(10,2))"""
        cursor.execute(sql_create)
        self.db_connection.commit()
        print("Tabla creada correctamente")

    def import_csv_to_db(self, csv_file):
        cursor = self.db_connection.cursor()
        df = pd.read_csv(csv_file)  
        print("Columnas en el CSV:", df.columns)  
        for i, row in df.iterrows():  
          
            values = tuple(row[col] for col in ['employee_id', 'department', 'performance_score', 'years_with_company', 'salary'])
            sql_insert = """INSERT INTO EmployeePerformance (employee_id, department, performance_score, years_with_company, salary)
                            VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(sql_insert, values)  
        self.db_connection.commit()  
        print("Datos importados correctamente")

    def get_dataframe(self):
        df = pd.read_sql_query("SELECT * FROM EmployeePerformance", self.db_connection)
        return df

class EmployeePerformanceAnalysis:
    def __init__(self, dataframe):
        self.df = dataframe
        self.departamentos = self.df['department'].unique()
        self.estadísticas = {}

    def calculate_statistics(self):
        for departamento in self.departamentos:
            df_departamento = self.df[self.df['department'] == departamento]  
            
          
            performance_score_media = df_departamento['performance_score'].mean()
            performance_score_mediana = df_departamento['performance_score'].median()
            performance_score_desviacion = df_departamento['performance_score'].std()

       
            salary_media = df_departamento['salary'].mean()
            salary_mediana = df_departamento['salary'].median()
            salary_desviacion = df_departamento['salary'].std()

   
            num_empleados = df_departamento.shape[0]

         
            correlacion_years_performance = df_departamento['years_with_company'].corr(df_departamento['performance_score'])


            correlacion_salary_performance = df_departamento['salary'].corr(df_departamento['performance_score'])

           
            self.estadísticas[departamento] = {
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

    def print_statistics(self):
        for departamento, estadística in self.estadísticas.items():
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

    def plot_histogram(self):
        for departamento in self.departamentos:
            df_departamento = self.df[self.df['department'] == departamento]
            plt.hist(df_departamento['performance_score'], bins=10, alpha=0.5, label=departamento)
        plt.xlabel('Performance Score')
        plt.ylabel('Frecuencia')
        plt.title('Histograma del Performance Score por Departamento')
        plt.legend()
        plt.show()

    def plot_scatter(self):
        plt.scatter(self.df['years_with_company'], self.df['performance_score'])
        plt.xlabel('Años con la empresa')
        plt.ylabel('Performance Score')
        plt.title('Gráfico de dispersión de years_with_company vs. performance_score')
        plt.show()

        plt.scatter(self.df['salary'], self.df['performance_score'])
        plt.xlabel('Salario')
        plt.ylabel('Performance Score')
        plt.title('Gráfico de dispersión de salary vs. performance_score')
        plt.show()


db_connection = DatabaseConnection(
    host="localhost",
    user="root",
    password="root",
    database="companydata",
    port="3208"
)
db = db_connection.connect()


employee_db = EmployeePerformanceDatabase(db)
employee_db.create_table()
employee_db.import_csv_to_db('./MOCK_DATA.csv')


df = employee_db.get_dataframe()


performance_analysis = EmployeePerformanceAnalysis(df)
performance_analysis.calculate_statistics()
performance_analysis.print_statistics()
performance_analysis.plot_histogram()
performance_analysis.plot_scatter()

db_connection.disconnect()
