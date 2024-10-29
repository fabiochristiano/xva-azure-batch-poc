import json

# Função para carregar e agregar os dados de um arquivo JSON
def load_and_aggregate(file_paths):
    aggregated_simulations = []
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            data = json.load(f)
            aggregated_simulations.extend(data["simulations"])
    return aggregated_simulations

def aggregate_and_save(input_files):
    # Carregar e agregar os dados dos arquivos
    aggregated_simulations = load_and_aggregate(input_files)

    # Salvar os dados agregados em um novo arquivo JSON
    with open('monte_carlo_result_aggregated.json', 'w') as f:
        json.dump({"simulations": aggregated_simulations}, f, indent=4)

    print(f"Dados agregados salvos em aggregated_monte_carlo_input.json")

def main():
    # Lista de arquivos a serem agregados
    input_files = [
        'monte_carlo_input_part_1.json',
        'monte_carlo_input_part_2.json',
        'monte_carlo_input_part_3.json',
        'monte_carlo_input_part_4.json'
    ]

    aggregate_and_save(input_files)

if __name__ == "__main__":
    main()