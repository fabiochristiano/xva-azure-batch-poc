import json
import os
from client import split_json
from worker import process_monte_carlo_simulations
from agreggator import load_and_aggregate

def orchestrate(input_file, num_nodes):
    # Dividir o arquivo de entrada
    split_json(input_file, num_nodes)
    
    # Processar cada parte
    for i in range(num_nodes):
        part_file = f'monte_carlo_input_part_{i+1}.json'
        process_monte_carlo_simulations(part_file)
    
    # Agregar os resultados
    aggregated_simulations = []
    for i in range(num_nodes):
        result_file = f'all_option_pricing_results_part_{i+1}.json'
        aggregated_simulations.extend(load_and_aggregate(result_file))
    
    # Salvar o resultado agregado em um novo arquivo JSON
    with open('aggregated_monte_carlo_results.json', 'w') as f:
        json.dump({"simulations": aggregated_simulations}, f, indent=4)
    
    print("Resultados agregados salvos em 'aggregated_monte_carlo_results.json'")

# Exemplo de uso
orchestrate('monte_carlo_input.json', 4)