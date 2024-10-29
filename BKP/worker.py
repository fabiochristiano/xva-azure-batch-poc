import numpy as np
import json

# Função de Simulação de Monte Carlo para Estimar o Valor de uma Opção de Compra
def monte_carlo_option_pricing(params):
    S0 = params["stock_price"]
    K = params["strike_price"]
    r = params["risk_free_rate"]
    sigma = params["volatility"]
    T = params["time_to_maturity"]
    num_simulations = params["num_simulations"]
    num_steps = params["num_steps"]

    dt = T / num_steps
    discount_factor = np.exp(-r * T)
    
    # Simulações de Monte Carlo
    option_values = np.zeros(num_simulations)
    
    for i in range(num_simulations):
        prices = np.zeros(num_steps + 1)
        prices[0] = S0
        
        for t in range(1, num_steps + 1):
            z = np.random.standard_normal()
            prices[t] = prices[t-1] * np.exp((r - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * z)
        
        option_values[i] = max(prices[-1] - K, 0)
    
    # Valor Esperado da Opção de Compra
    expected_option_value = np.mean(option_values) * discount_factor
    
    # Cálculo do Intervalo de Confiança
    std_error = np.std(option_values) / np.sqrt(num_simulations)
    confidence_interval = (expected_option_value - 1.96 * std_error, expected_option_value + 1.96 * std_error)
    
    return {
        "expected_option_value": expected_option_value,
        "confidence_interval": list(confidence_interval),  # Convertendo tupla para lista
        "option_values": option_values.tolist()
    }

# Função para processar as simulações de Monte Carlo a partir de um arquivo JSON
def process_monte_carlo_simulations(input_file):
    # Carregar a lista de simulações do JSON de entrada
    with open(input_file, 'r') as f:
        data = json.load(f)
        simulations = data["simulations"]

    # Executar a Simulação para cada conjunto de parâmetros na lista
    results = []
    for simulation in simulations:
        params = simulation["parameters"]
        result = monte_carlo_option_pricing(params)
        simulation["results"] = result
        results.append(simulation)

    # Converter todos os resultados para JSON
    result_json = json.dumps({"simulations": results}, indent=4)

    # Salvar em um arquivo JSON
    output_file = 'all_option_pricing_results.json'
    with open(output_file, 'w') as f:
        f.write(result_json)

    print(f"Resultados salvos em '{output_file}'")
    print(result_json)

# Exemplo de chamada da função
process_monte_carlo_simulations('monte_carlo_input.json')
