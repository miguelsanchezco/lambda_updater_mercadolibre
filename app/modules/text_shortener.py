
def text_shortener(text, characters):
    # Programa acortador de titulos a una cantidad de caracteres, characters
    
    len_title = len(text)  
    if len_title <= characters:
        pass
    # print('text OK 1')
    # print('text definitivo: ', text)
    # print('len_title: ', len(text))
    elif text[characters:][0] == " " or text[:characters][-1] == " " :
        text = text[:characters]
    # print('text OK 2')
    # print('text definitivo: ', text)
    # print('len_title: ', len(text))
    else:
        for i in reversed(range(characters)):
            cutting_title = text[:characters][i]
            if cutting_title == " ":
                text = text[:i]
                # print('text definitivo: ', text)
                # print('len_title: ', len(text))
                break   
    return text