import json

def split_json(input_file, num_nodes):
    with open(input_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    simulations = data['simulations']
    total_simulations = len(simulations)
    chunk_size = (total_simulations + num_nodes - 1) // num_nodes  # Calcula o tamanho de cada parte

    for i in range(num_nodes):
        start_index = i * chunk_size
        end_index = min(start_index + chunk_size, total_simulations)
        chunk = simulations[start_index:end_index]
        
        output_data = {'simulations': chunk}
        output_file = f'monte_carlo_input_part_{i+1}.json'
        
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(output_data, outfile, ensure_ascii=False, indent=4)

# Exemplo de uso
split_json('monte_carlo_input.json', 4)
#split_json('all_option_pricing_results.json', 4)
