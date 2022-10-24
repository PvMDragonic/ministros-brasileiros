from multiprocessing import Process, Manager
from threading import Thread
from lxml import html
import requests
import csv

def dividir_lista(quantidade, lista):
    return [lista[i::quantidade] for i in range(quantidade)]

def buscar_ministros(lista, todas_listas):
    for link in lista:
        conteudo = html.fromstring(requests.get(link).content)

        data = conteudo.xpath('.//li[@id="footer-info-lastmod"]')[0].text_content()
        data = data.split("de")
        data = "de".join(data[1:])[1:-1]

        tabela = conteudo.xpath('.//table//tbody//tr')
        tabela = list(tabela)
        for linha in tabela:
            # Transforma os HtmlElements em String.
            text = [elem.text_content() for elem in linha]

            # Filtra dados nulos ou vazios.
            text = [elem for elem in text if len(elem) > 2 and elem != "?"]

            # Muito lixo vem como uma linha de elemento único
            if len(text) > 1:
                # Tudo vem con newline no final.
                text = [elem[:-1] for elem in text if elem[-1] == "\n"]

                # Filtra siglas e outras coisas, vendo se tem caractere maiúsculo antecedido por letra. Ex.: 'SeCom'; 'PCdoB'; 'PT'; etc.
                text = [elem for elem in text if not any(elem[i].isupper() and elem[i-1] not in (" ", "-") for i in range(1, len(elem)))]

                # O que manteve newline (porque não tava no final) é lixo.
                text = [elem for elem in text if not "\n" in elem]

                # Filtra números (índice da tabela; valores monetários; etc) mas deixa datas (por extenso) passar.
                text = [elem for elem in text if not (any(char.isdigit() for char in elem) and not ' de ' in elem)]

                # Ignora o header de cada tabela.
                if any(elem for elem in text if elem in ("Ministério", "Partido")):
                    continue

                # Se não sobrou nada/um elemento depois da filtragem, é lixo.
                if len(text) < 2:
                    continue

                # Checa se não tem "DD de MM de AAAA" na linha.
                if text[-1].count(" de ") != 2:
                    text.append(data)

                # Versões inicias da página não tinham "Ministério".
                if text[0][0].islower():
                    text[0] = f"Ministério {text[0]}"

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
            processos.append(Process(target = processo, args = (sublista, todas_listas)))

        for p in processos:
            p.start()

        for p in processos:
            p.join()

    with open('ministros.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for elem in todas_listas:
            writer.writerow(elem) 