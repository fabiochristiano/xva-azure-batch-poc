from client import split_json
from agreggator import aggregate_and_save
from worker import run_batch_process

def orchestrate(input_file, num_nodes):
    
    # Dividir o arquivo de entrada
    files_input = split_json(input_file, num_nodes)
    
    # Montar a lista de arquivos que será processado para agregar no final
    files_output = [file.replace('input', 'result') for file in files_input]
     
    # Rodar o processo batch
    run_batch_process(files_input)
        
    # Agregar os resultados
    aggregate_and_save(files_output)

def main():
    # Exemplo de uso
    orchestrate('src/files/input/monte_carlo_input.json', 4)

if __name__ == "__main__":
    main()