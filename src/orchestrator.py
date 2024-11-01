from client import split_json
from agreggator import aggregate_and_save
from worker import run_batch_process

def orchestrate(input_file):
    
    # Dividir o arquivo de entrada
    files_input = split_json(input_file)
    
    # Montar a lista de arquivos que será processado para agregar no final
    files_output_path = [file.replace('input', 'result') for file in files_input]
    files_output = [file.replace('src/files/temp/', '') for file in files_output_path]
    
     
    # Rodar o processo batch
    run_batch_process(files_input)
        
    # Agregar os resultados
    aggregate_and_save(files_output)

def main():
    # Exemplo de uso
    orchestrate('monte_carlo_input.json')

if __name__ == "__main__":
    main()