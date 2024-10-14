from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Función que imprime un mensaje
def print_hello():
    print("Hello from Airflow!")

# Función que realiza una suma simple
def add_numbers():
    result = 5 + 7
    print(f"El resultado de la suma es: {result}")
    return result

# Función que imprime un mensaje dependiente
def print_result():
    print("La operación anterior fue exitosa.")

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2024, 10, 10),  # Fecha de inicio del DAG
}

# Crear el DAG
with DAG(
    'mi_dag_ejemplo1',  # Nombre del DAG
    default_args=default_args,
    description='Un DAG simple en Airflow',
    schedule_interval=timedelta(minutes=1),  # Se ejecuta diariamente
    catchup=False,  # No ejecuta DAGs pasados si la fecha ya pasó
) as dag:

    # Definir las tareas
    tarea_1 = PythonOperator(
        task_id='imprimir_hello',  # Identificador de la tarea
        python_callable=print_hello  # Función que ejecutará
    )

    tarea_2 = PythonOperator(
        task_id='sumar_numeros',
        python_callable=add_numbers
    )

    tarea_3 = PythonOperator(
        task_id='imprimir_resultado',
        python_callable=print_result
    )

    # Definir las dependencias entre las tareas
    tarea_1 >> tarea_2 >> tarea_3  # Se ejecutan en secuencia