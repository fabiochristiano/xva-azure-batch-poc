import json
import azure_impl.storage_impl as storage_impl


# Função para carregar e agregar os dados de um arquivo JSON
def load_and_aggregate(file_paths):
    aggregated_simulations = []
    for file_path in file_paths:
        storage_impl.get_file_from_container('temp', file_path, f'src/files/temp/{file_path}')
        with open(f'src/files/temp/{file_path}', 'r') as f:
            data = json.load(f)
            aggregated_simulations.extend(data["simulations"])
        
        
    return aggregated_simulations

def aggregate_and_save(input_files):
    # Carregar e agregar os dados dos arquivos
    aggregated_simulations = load_and_aggregate(input_files)

    # Salvar os dados agregados em um novo arquivo JSON
    with open('src/files/output/monte_carlo_result_aggregated.json', 'w') as f:
        json.dump({"simulations": aggregated_simulations}, f, indent=4)
     
    input_container_name = 'output'
    storage_impl.create_container_if_not_exists(input_container_name)  # Use the new function   
    storage_impl.upload_file_to_container('output', 'src/files/output/monte_carlo_result_aggregated.json')

    print(f"Dados agregados salvos em aggregated_monte_carlo_input.json")

def main():
    # Exemplo de uso
    input_files = [
        'monte_carlo_result_part_1.json',
        'monte_carlo_result_part_2.json',
        'monte_carlo_result_part_3.json',
        'monte_carlo_result_part_4.json',
    ]

    aggregate_and_save(input_files)

if __name__ == "__main__":
    main()