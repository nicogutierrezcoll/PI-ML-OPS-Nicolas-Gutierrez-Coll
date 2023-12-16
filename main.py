from fastapi import FastAPI
import pandas as pd

## Generar la carga de todos los csv usados para las funciones!
# Funcion 1
df_funcion_PlaytimeGenre = pd.read_csv('df_funciones/df_funcion_PlaytimeGenre.csv')
# Funcion 2
df_funcion_UserForGenre = pd.read_csv('df_funciones/df_user_for_genre.csv')
# Funcion 3
users_reviews_fc3 = pd.read_csv('df_funciones/users_reviews_fc3.csv')
listado_juegos_sin_repetidos = pd.read_csv('df_funciones/listado_juegos_sin_repetidos.csv')
# Funcion 4
df_funcion_UsersWorstDeveloper = pd.read_csv('df_funciones/df_funcion_UsersWorstDeveloper.csv')
# Funcion 5
df_funcion_sentiment_analysis = pd.read_csv('df_funciones/df_funcion_sentiment_analysis.csv')


app = FastAPI()

@app.get("/")
async def root():
    return {"mensaje": "¡Bienvenido a mi API!"}


## PRIMER FUNCIÓN: PLAY TIME GENRE

@app.get('/PlayTimeGenre/{genero}')
def PlayTimeGenre(genero: str):

    # Filtrar el DataFrame por el género especificado
    df_genero = df_funcion_PlaytimeGenre[df_funcion_PlaytimeGenre['genres'] == genero]

    # Agrupar por año y calcular la suma de las horas jugadas
    resumen_por_anio = df_genero.groupby('year')['playtime_forever'].sum()

    # Encontrar el año con más horas jugadas
    anio_con_mas_horas = resumen_por_anio.idxmax()

    # Encontrar la cantidad total de horas jugadas en ese año
    horas_totales = resumen_por_anio.max()

    return f"""
Género: {genero}
Año con más horas jugadas para el género: {anio_con_mas_horas}.
Horas totales jugadas: {horas_totales}
"""


## SEGUNDA FUNCIÓN: USERS FOR GENERO

@app.get('/UserForGenre/{genero}')
def UserForGenre(genero: str):
    
    # Filtrar el DataFrame por el género especificado
    df_genero = df_funcion_UserForGenre[df_funcion_UserForGenre['genres'] == genero]

    # Agrupar por usuario y calcular la suma de las horas jugadas
    resumen_por_user = df_genero.groupby('user_id')['playtime_forever'].sum()

    # Encontrar el usuario con más horas jugadas
    user_con_mas_horas = resumen_por_user.idxmax()

    # Encontrar la cantidad total de horas jugadas por el usuario
    horas_totales = resumen_por_user.max()

    return f"""
Género: {genero}
Usuario con más horas jugadas para el género: {user_con_mas_horas}.
Horas totales jugadas: {horas_totales}
"""

## TERCER FUNCIÓN: USERS RECOMMEND

@app.get('/UsersRecommend/{anio}')
def UsersRecommend(año: int):
    # Calcula el margen de años donde hay datos
    año_maximo = users_reviews_fc3['year'].max()
    año_min = users_reviews_fc3['year'].min()

    if año < año_min or año > año_maximo:
        
        print('No hay datos para el año indicado!')
        print('Se debe colocar un año entre', año_min, 'y', año_maximo)
    
    else:
        # Filtro el año dado con recomendación igual a 'True' y sentimiento positivo/neutro
        filtro_reviews = users_reviews_fc3[(users_reviews_fc3['year'] == año) &
                                        (users_reviews_fc3['recommend'] == True) &
                                        (users_reviews_fc3['sentiment_analysis'] >= 1)]

        # Agrupo por item_id y suma el sentiment_analysis
        sentiment_sum = filtro_reviews.groupby('item_id')['sentiment_analysis'].sum().reset_index(name='sentiment_sum')

        # Ordeno de forma descendente según la suma de 'sentiment_sum'
        ordenados_sentiments = sentiment_sum.sort_values(by='sentiment_sum', ascending=False)

        # Asocia los item_id con los nombres de los juegos
        df_merge = pd.merge(ordenados_sentiments, listado_juegos_sin_repetidos, on='item_id', how='left')

        # Elimino las filas con NaN
        df_merge = df_merge.dropna()

        # Filtro el top 3
        top_3_con_nombres = df_merge.head(3)

        # Crea la lista de resultados en el formato deseado con nombres de juegos
        result = [{"Puesto {}: ".format(i+1): item_name} for i, item_name in enumerate(top_3_con_nombres['item_name'])]

        return result


## CUARTA FUNCIÓN: USER WORST DEVELOPER

@app.get('/UsersWorstDeveloper/{anio}')
def UsersWorstDeveloper(año: int):
    # Calcula el margen de años donde hay datos
    año_maximo = df_funcion_UsersWorstDeveloper['year'].max()
    año_min = df_funcion_UsersWorstDeveloper['year'].min()

    if año < año_min or año > año_maximo:
        
        print('No hay datos para el año indicado!')
        print('Se debe colocar un año entre', año_min, 'y', año_maximo)
    
    else:
        # Filtro el año dado con recomendación igual a 'False' y sentimiento negativo
        filtro_reviews = df_funcion_UsersWorstDeveloper[
            (df_funcion_UsersWorstDeveloper['year'] == año) &
            (df_funcion_UsersWorstDeveloper['recommend'] == False) &
            (df_funcion_UsersWorstDeveloper['sentiment_analysis'] == 0)]

        # Agrupo por item_id y cuento la cantidad de comentarios negativos
        sentiment_count = filtro_reviews.groupby('developer')['sentiment_analysis'].count().reset_index(name='sentiment_count')

        # Ordeno de forma descendente el conteo de 'sentiment_count'
        ordenados_sentiments = sentiment_count.sort_values(by='sentiment_count', ascending=False)

        # Filtro el top 3
        top_3_worst_developer = ordenados_sentiments.head(3)

        # Crea la lista de resultados en el formato deseado con nombres de juegos
        result = [{"Puesto {}: ".format(i+1): item_name} for i, item_name in enumerate(top_3_worst_developer['developer'])]

        return result
    

## QUINTA FUNCION: SENTIMENTS_ANALYSIS

@app.get('/sentiment_analysis/{developer}')
def sentiment_analysis(developer: str):
    # Genero un filtro por developer que entra como input
    df_filtro_fc5 = df_funcion_sentiment_analysis[(df_funcion_sentiment_analysis['developer'] == developer)]

    # Contar las filas donde sentiment_analysis es igual a 0, generando un booleano y luego suma ese True
    valor_0 = int(df_filtro_fc5['sentiment_analysis'].eq(0).sum())
    valor_1 = int(df_filtro_fc5['sentiment_analysis'].eq(1).sum())
    valor_2 = int(df_filtro_fc5['sentiment_analysis'].eq(2).sum())

    # Crea el diccionario para la respuesta
    resultados_dict = {developer: {'Positive': valor_2, 'Neutral': valor_1, 'Negative': valor_0 }}

    return resultados_dict

