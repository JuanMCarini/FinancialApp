import os
from unittest.mock import patch

# Change working directory to the project root
os.chdir(r"c:\Users\juanm_8qa8lav\Documents\Proyectos_Personales\FinancialApp")

# Lista de entradas para simular
inputs = ['Empresa dueña de la cartera.',
          'Proveedor de carteras de créditos.',
          'Nos consiguen clientes de Chubut.', 0.11, 0.0, 0.06, 0.08, 0.095, 0.0135, 0.0145, 0.0, 0.0,
          'Cambio de comisiones.', 0.06, 0.0, 0.06, 0.08, 0.095, 0.0135, 0.0145, 0.0, 0.0]

for i in range(27):
    inputs.append('Yes')

# Función que simula input() devolviendo un valor de la lista cada vez que se llama
def mock_input(prompt):
    return inputs.pop(0)

# Parchear input() con la función mock_input
with patch('builtins.input', mock_input):
    with open('logs/log.py', 'r', encoding='utf-8') as file:
        exec(file.read())