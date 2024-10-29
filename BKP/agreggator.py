import json

# Função para carregar e agregar os dados de um arquivo JSON
def load_and_aggregate(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data["simulations"]

# Carregar os dados dos arquivos
simulations_part_1 = load_and_aggregate('monte_carlo_input_part_1.json')

# Agregar os dados
aggregated_simulations = simulations_part_1

# Salvar os dados agregados em um novo arquivo JSON
with open('aggregated_monte_carlo_input.json', 'w') as f:
    json.dump({"simulations": aggregated_simulations}, f, indent=4)

print("Dados agregados salvos em 'aggregated_monte_carlo_input.json'")