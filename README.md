**Table of Contents**

[TOCM]

[TOC]

# Processamento dos logs da NASA

### Dados utilizados
Para a construção do job que analisa métricas dos logs de requisições ao servidor da NASA foram utilizados os dataset de logs gerados entre 01 de julho até 31 de agosto de 1995.

Esses dados de julho estão disponíveis [aqui](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz) e os de agosto [aqui](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)

### Estrutura do código
O job é constituído por dois arquivos, main e pipeline.  Esses arquivos cuidam da interação com o usuário e da construção do job Spark e devidos cálculos, respectivamente.

### Métricas calculadas
São calculadas 5 métricas. São elas:
- Número de hosts únicos.
- Total de erros 404.
- As 5 URLs que mais causaram erro 404.
- Quantidade de erros 404 por dia.
- O total de bytes retornados.

### Utilização do job
Para a execução é necessário informar alguns parâmetros, que estão listados abaixo. 

| Parâmetro |  Required | Valor Padrão | Descrição
| ------------- | ------------- | ------------- | ------------- |
| master  | Não | local | Determina onde o job será executado | 
| app-name  | Não | spark-job-yyyymmdd-HHMMSS  | Nome do job |
| input-file  |  Sim | Não há | Arquivo que será utilizado como entrada. Para passar mais do que 1 arquivo por vez deve-se usar vírgula (,) para separá-los, e.g. ```--input-file /usr/local/file1,/usr/local/file2```  |
| output-dir  | Sim | Não há  | Diretório no qual os arquivos com as métricas serão escritos. Cada uma das métricas irá criar seu próprio diretório para armazenar seus resultados |
| jobs  | Não | todos  | Seleciona quais métricas serão cálculadas, qualquer combinação é aceita. A lista de jobs disponíveis é:  **hosts-unicos, total-404, top-5-url-404, 404-dia, total-bytes**|
| log-level  | Sim | Não há  | Define o nível de log utilizado para a aplicação. Uma das seguintes opções deve ser selecionada: **off, fatal, error, warn, info, debug, trace, all** |

Para a correta execução do job é necessário incluir no comando **spark-submit** o parâmetro **py-files** com o valor sendo o caminho até o arquivo **pipeline.py**

Um exemplo de submissão, partindo da pasta na qual a aplicação está, é:
```
spark-submit --py-file pipeline.py main.py --master local[*] --input-file /usr/local/NASA_access_log_Jul95.gz,/usr/local/NASA_access_log_Aug95.gz --output-dir /usr/local/output --jobs host-unicos,total-404 --log-level error
```

Após a finalizada a execução, considerando que foram selecionadas todas as métricas, a pasta output terá essa configuração:


Vale notar que o nome das pastas criadas variam de acordo com o momento no qual o job foi executado.

# Questões

##### Qual é o objetivo do comando **cache** em Spark?
O comando cache é utilizado para guardar os RDDs/Datasets/Dataframes na memória principal (RAM). Caso a quantidade de dados exceda a capacidade da memória principal o Spark utilizará a memória secundária (SSD, HDD) para esse excedente.

##### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Jobs MapReduce tem por característica guardar os dados gerados durante a execuação do job na memória secundária, já o Spark faz proveito da memória primária, cujo acesso é mais rápido. Essa diferença faz com que os jobs escritos em Spark sejam geralmente mais rápidos do que jobs MapReduce.

##### Qual é a função do **SparkContext**?
SparkContext, com a ajuda dos gerenciadores de recursos, conecta a aplicação Spark com o cluster e demais recursos.

##### Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
O RDD é a estrutura de dados base do Spark sobre qual todas as outras foram construídas.

##### **GroupByKey** é menos eficiente que **reduceByKey** em grandes datasets. Por quê?
O Spark é um framework para realização de computação parelela, ou seja, distriuída em diversos nós. Cada um desses nós recebe o código a ser executado e uma parte dos dados. Quando utilizamos o comando GroupByKey forçamos a transferência de todos os dados com uma mesma chave para o nó responsável pelo seu processamento, dependendo da característica dos dados em questão podemos gerar um aumento excessivo do tráfego dos dados na rede e/ou podemos acabar sobrecarregando um desses nós. Essa sobrecarga em um dos nós faz com que todos os outros fiquem ociosos enquanto aguardam a finalização do seu processamento e pode até mesmo gerar falhas de execução.
O reduceByKey tem uma abordagem diferente para esse problema. Cada nó agrupa os dados disponíveis para si e passa o resultado adiante, o próximo nó agrupa somente os dados que foram disponibilizados para ele e assim por diante, até que tenhamos valores únicos para cada uma das chaves.

##### Explique o que o código Scala abaixo faz. 
```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_+_)
counts.saveAsTextFile("hdfs://...")
```
Esse programa conta a quantidade de vezes que cada palavra aparece no arquivo do input e grava o resultado no HDFS. A primeira linha do código lê um arquivo armazenado no HDFS, a segunda linha separa o conteúdo do arquivo nos espaços e retorna cada um dos tokens em uma linha distinta. Em seguida criasse o par chave e valor, sendo a chave a token criado no comando anterior e o valor o inteiro um. Por último soma os valores e agrupa as chaves para então gravar um arquivo com os resultados no HDFS.

