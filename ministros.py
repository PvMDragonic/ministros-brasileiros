from multiprocessing import Process, Manager
from unidecode import unidecode
from threading import Thread
from time import sleep
from lxml import html
import requests
import datetime
import csv
import re

def dividir_lista(quantidade, lista):
    return [lista[i::quantidade] for i in range(quantidade)]

def buscar_ministros(lista, todas_listas):
    def arrumar_data(data):
        meses = {
            'janeiro': '01',
            'fevereiro': '02',
            'março': '03',
            'abril': '04',
            'maio': '05',
            'junho': '06',
            'julho': '07',
            'agosto': '08',
            'setembro': '09',
            'outubro': '10',
            'novembro': '11',
            'dezembro': '12'
        }

        data = data.replace(" de ", "-")
        data = data.split("-")
        return datetime.datetime.strptime(f"{data[2]}-{meses[data[1].lower()]}-{data[0]}", "%Y-%m-%d").date()

    def criar_chave(elemento):
        chave = f"{elemento[0]} {elemento[1]}"
        chave = unidecode(chave)
        for stopword in (" da ", " de ", " do ", " das ", " dos "):
            if stopword in chave:
                chave = chave.replace(stopword, " ")
        return chave.lower()

    for link in lista:
        while True:
            try:
                # Às vezes o request dá erro por algum motivo, daí é só esperar e tentar de novo.
                conteudo = html.fromstring(requests.get(link).content)
                break
            except Exception:
                sleep(1)

        data = conteudo.xpath('.//li[@id="footer-info-lastmod"]')[0].text_content()
        data = data.split("de")
        data = "de".join(data[1:])[1:-1]
        data = arrumar_data(data)

        tabela = conteudo.xpath('.//table//tbody//tr')
        tabela = list(tabela)
        for linha in tabela:
            # Transforma os HtmlElements em String.
            text = [elem.text_content() for elem in linha]

            # Filtra dados nulos ou vazios.
            text = [elem for elem in text if len(elem) > 2 and elem != "—"]
            
            # Muito lixo vem como uma linha de elemento único
            if len(text) > 1:
                # Tudo vem con newline no final.
                text = [elem[:-1] for elem in text if elem[-1] == "\n"]

                # Filtra siglas e outras coisas, vendo se tem caractere maiúsculo antecedido por letra. Ex.: 'SeCom'; 'PCdoB'; 'PT'; etc.
                text = [elem for elem in text if not any(elem[i].isupper() and elem[i-1] not in (" ", "-") for i in range(1, len(elem)))]

                # O que manteve newline (porque não tava no final) é lixo.
                text = [elem for elem in text if not "\n" in elem]

                # Remove mais informação inútil das linhas.
                lista_dejetos = ("carece de", "sem partido", "vago", "independente", "epublican", "união brasil", "gressistas", "atriota", "definir", "pps", "  ", "")
                text = [elem for elem in text if not any([string for string in lista_dejetos if string in elem.lower()])]

                # Filtra números (índice da tabela; valores monetários; etc) mas deixa datas (por extenso) passar.
                text = [elem for elem in text if not (any(char.isdigit() for char in elem) and not ' de ' in elem)]

                # Removendo adendos inúteis aos nomes das pessoas.
                for index, elem in enumerate(text):
                    for palavra in ("(interino)", "(posse pendente)"):
                        if palavra in elem:
                            text[index] = text[index].replace(palavra, "")

                # A remoção dos adendos pode deixar um espaço residual na frente do nome das pessoas.
                text = [elem[:-1] if elem[-1] == " " else elem for elem in text]

                # Ignora o header de cada tabela.
                if any(elem for elem in text if elem in ("Ministério", "Partido")):
                    continue

                # Se não sobrou nada/um elemento depois da filtragem, é lixo.
                if len(text) < 2:
                    continue
                
                # Tira a referência de rodapé do texto bruto.
                if text[-1].endswith("]"):
                    text[-1] = text[-1][:-3]

                # Tem uma única ocorrência onde o jégue colocou o ano como "2pww".
                if text[-1].endswith("pww"):
                    text[-1] = f"{text[-1][:-3]}022"

                # Outro caboclo colocou "[...]2022 até a atualidade" na data.
                if text[-1].endswith("atualidade"):
                    text[-1] = text[-1][:-17]

                # Procura por "DD de MM de AAAA".
                if re.search("[0-9]+ de [a-zç]{4,} de [0-9]{4}", text[-1].lower()):
                    text[-1] = arrumar_data(text[-1])
                else:
                    text.append(data)

                # Versões inicias da página não tinham "Ministério".
                if text[0][0].islower():
                    text[0] = f"Ministério {text[0]}"
           
                if len(text) < 3:
                    #print([elem.text_content() for elem in linha])
                    continue

                text.insert(0, criar_chave(text))

                todas_listas.append(text)
                print(text)

def processo(lista, todas_listas):
    listas = dividir_lista(2, lista)
    for sublista in listas:
        Thread(target = buscar_ministros, args = (sublista, todas_listas, )).start()

def arrumar_datas(sem_duplicatas: list) -> list:
    def separar_conjunto(sem_duplicatas: list, inicio: int) -> tuple:
        temp = 0
        conjunto = []
        chave_da_vez = None
        
        # Separa os elementos semelhantes.
        for i in range(inicio, len(sem_duplicatas)):
            temp = i

            if not isinstance(sem_duplicatas[i][-2], datetime.date) and chave_da_vez == None:
                chave_da_vez = sem_duplicatas[i][0]

            if sem_duplicatas[i][0] != chave_da_vez and chave_da_vez != None:
                inicio = i
                return (conjunto, inicio)

            conjunto.append(sem_duplicatas[i])
        
        # Caso termine o loop sem dar return.
        inicio = temp
        return (conjunto, inicio)

    def corrigir_conjunto(lista_corrigida: list, conjunto: list) -> list:
        while True:
            if len(conjunto) == 0:
                break

            para_deletar = []
            periodo_selecionado = None

            for elem in conjunto:
                if periodo_selecionado == None:
                    periodo_selecionado = elem[-1] + datetime.timedelta(days = 1461) # 4 anos.

                if elem[-1] < periodo_selecionado:
                    para_deletar.append(elem)

            conjunto = [elem for elem in conjunto if elem not in para_deletar]       

            para_deletar = sorted(para_deletar, key = lambda lista: lista[-1])
            
            lista_corrigida.append([
                para_deletar[0][1],
                para_deletar[0][2],
                para_deletar[0][3],
                para_deletar[-1][-1]
            ])

        return lista_corrigida

    lista_corrigida = []
    inicio = 0

    while True:
        conjunto, inicio = separar_conjunto(sem_duplicatas, inicio)
        lista_corrigida = corrigir_conjunto(lista_corrigida, conjunto)

        if inicio == len(sem_duplicatas) - 1:
            break
        
    return lista_corrigida

def resolver_datas_iguais(lista: list) -> list:
    for elem in lista:
        if elem[-2] != elem[-1]:
            continue

        for item in lista:
            # Pula ministérios diferentes.
            if elem[0] != item[0]:
                continue
            # Pula o próprio.
            if elem == item:
                continue

            if (item[-2] - elem[-1]) >= datetime.timedelta(days = 1095): # 3 anos.          
                elem[-1] = item[-2] - datetime.timedelta(days = 1)
                
        # Passou batido por todas validações.
        if elem[-2] == elem[-1]:         
            elem[-1] = elem[-2] + datetime.timedelta(days = 1095)

    return lista

if __name__ == "__main__":
    # Essa lista vai ficar defasada, com o passar do tempo; TO-DO: fazer isso ficar dinâmico, puxando direto da Wikipédia.
    historico_wikipedia = [
        'https://pt.wikipedia.org/w/index.php?title=Minist%C3%A9rios_do_Brasil&action=history&dir=prev&offset=20160601211004%7C45771511&limit=500',
        'https://pt.wikipedia.org/w/index.php?title=Minist%C3%A9rios_do_Brasil&action=history&offset=20160602031248%7C45774870&limit=500',
        'https://pt.wikipedia.org/w/index.php?title=Minist%C3%A9rios_do_Brasil&action=history&offset=20071008021452%7C7783981&limit=500'
    ]

    todas_listas = Manager().list()

    for pagina in historico_wikipedia:
        wikipedia = html.fromstring(requests.get(pagina).content)
        lista_historico = wikipedia.xpath('.//section[@id="pagehistory"]')[0]
        lista_historico = lista_historico.findall('ul//li//a')
        lista_historico = [row.get("href") for row in lista_historico]
        lista_historico = [row for row in lista_historico if 'oldid=' in row and not 'diff=' in row]
        lista_historico = [f'https://pt.wikipedia.org/{row}' for row in lista_historico]

        lista_historico = dividir_lista(5, lista_historico)
        processos = []

        for sublista in lista_historico:
            processos.append(
                Process(target = processo, args = (sublista, todas_listas))
            )

        for p in processos:
            p.start()

        for p in processos:
            p.join()
    
    sem_duplicatas = list(set(tuple(element) for element in list(todas_listas)))
    sem_duplicatas = sorted(sem_duplicatas, key = lambda lista: lista[0])
    sem_duplicatas = arrumar_datas(sem_duplicatas)

    # Alguns elementos passam por todas as filtragens, coisa de uns 10.
    # Como alguns deles ainda parecem estar preenchidos com dados 
    # errados/duvidosos, vai tudo pra vala.
    for elem in sem_duplicatas:
        if type(elem[-2]) == str:
            sem_duplicatas.remove(elem)

    sem_duplicatas = resolver_datas_iguais(sem_duplicatas)

    final = sorted(
        sem_duplicatas, 
        key = lambda lista: lista[0]
    )

    with open(f'ministros_{datetime.datetime.now().date()}.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for elem in final:
            writer.writerow(elem)