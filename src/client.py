import json
import azure.storage_impl as storage_impl

def split_json(input_file_name):
    
    input_container_name = 'input'
    storage_impl.create_container_if_not_exists(input_container_name)  # Use the new function
    storage_impl.get_file_from_container('input', input_file_name, f'src/files/input/{input_file_name}')
    
    with open(f'src/files/input/{input_file_name}', 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    num_nodes = 4
    simulations = data['simulations']
    total_simulations = len(simulations)
    chunk_size = (total_simulations + num_nodes - 1) // num_nodes  # Calcula o tamanho de cada parte

    output_files = []

    for i in range(num_nodes):
        start_index = i * chunk_size
        end_index = min(start_index + chunk_size, total_simulations)
        chunk = simulations[start_index:end_index]
        
        output_data = {'simulations': chunk}
        output_file = f'src/files/temp/monte_carlo_input_part_{i+1}.json'
        
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(output_data, outfile, ensure_ascii=False, indent=4)
        
        output_files.append(output_file)

    return output_files

def main():
    # Exemplo de uso
    split_json('monte_carlo_input.json', 4)

if __name__ == "__main__":
    main()