from multiprocessing import Process, Manager
from datetime import datetime
from threading import Thread
from time import sleep
from lxml import html
import requests
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
        return datetime.strptime(f"{data[2]}-{meses[data[1].lower()]}-{data[0]}", "%Y-%m-%d").date()

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
                text = [elem for elem in text if elem.lower() not in ("cargo vago", "sem partido", "vago")]

                # Filtra números (índice da tabela; valores monetários; etc) mas deixa datas (por extenso) passar.
                text = [elem for elem in text if not (any(char.isdigit() for char in elem) and not ' de ' in elem)]

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

                todas_listas.append(text)
                print(text)

def processo(lista, todas_listas):
    listas = dividir_lista(2, lista)
    for sublista in listas:
        Thread(target = buscar_ministros, args = (sublista, todas_listas, )).start()

if __name__ == "__main__":
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
    
    final = sorted(list(todas_listas), key = lambda lista: lista[-1])

    with open('ministros.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for elem in final:
            writer.writerow(elem)