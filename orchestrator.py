from client import split_json
from worker import process_monte_carlo_simulations
from agreggator import aggregate_and_save

def orchestrate(input_file, num_nodes):
    # Dividir o arquivo de entrada
    files_input = split_json(input_file, num_nodes)
    # montar a lista de arquivos que será processado para agregar no final
    files_output = [file.replace('input', 'result') for file in files_input]

    # Processar cada parte
    for i in range(num_nodes):
        part_file = f'monte_carlo_input_part_{i+1}.json'
        process_monte_carlo_simulations(part_file)
        
    # Agregar os resultados
    aggregate_and_save(files_output)

def main():
    # Exemplo de uso
    orchestrate('monte_carlo_input.json', 4)

if __name__ == "__main__":
    main()