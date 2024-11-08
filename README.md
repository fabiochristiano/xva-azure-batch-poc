# xva-azure-batch-poc

## Descrição

Esta aplicação realiza simulações de Monte Carlo para estimar o valor de opções de compra utilizando o Azure Batch para processamento distribuído.

## Funcionalidades

- Simulação de Monte Carlo para opções de compra
- Processamento distribuído utilizando Azure Batch
  - Criação de pools de máquinas virtuais
  - Criação de jobs e associação com pools
  - Adição de tarefas aos jobs
  - Monitoramento e espera pela conclusão das tarefas
  - Impressão dos resultados das tarefas
  - Listagem de aplicações no Azure Batch
  - Obtenção da última versão de uma aplicação no Azure Batch
  - Deleção de todos os pools e jobs
- Armazenamento de dados no Azure Storage

## Requisitos

- Conta no Azure
- Azure Batch
- Azure Storage
- Python 3.x

## Configuação

O passo a passo de como configurar o ambiente e aplicação se encontra em [xva-azure-batch-poc.drawio](diagrama/xva-azure-batch-poc.drawio)


## Execução

O passo a passo da execução da aplicação está detalhado no diagrama abaixo:

![Execução da aplicação](diagrama/xva-azure-batch-poc-Execução%20da%20aplicação.drawio.png)

