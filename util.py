import pandas as pd
import os
import requests
import warnings
from pathlib import Path
warnings.filterwarnings("ignore")


# Här följer alla huvudsakliga beräkningar till Dagens Samhälles 
# valnummer 2018: 

def gov_mandates(year):
    
    slask = ['BLANK','OG','OGEJ','övriga_mindre_partier_totalt']
    
    path_styren = Path('data/styren_2006_2014_formatted.xlsx')

    # hämta valdata
    styren = pd.read_excel(path_styren)
    
    # se till att alla block är i stora bokstäver
    styren.block = styren.block.str.upper().str.strip()
    
    # filtrera rätt år
    styren=styren.loc[styren.valår == year].iloc[:,1:]
    
    # sortera bort onödiga kolumner
    partistyren = styren.loc[:,['kommun','styre']]
    
    # se till att alla styren är i stora bokstäver
    partistyren.styre = partistyren.styre.str.upper()
    
    # här splittas kolumnen med styren så att varje 
    # parti får en egen kolumn
    partier=pd.concat([partistyren,
                    partistyren.styre.str.split(',',expand=True)],
                   axis=1)
    
    # kolumnen 'styre' behövs inte längre
    del partier['styre']
    
    # lägg ihop alla kolumner med partier till en enda så
    # alla partier är i samma kolumn
    partier=partier.melt(id_vars='kommun').iloc[:,[0,-1]]
    
    # bort med alla null-värden
    partier=partier.loc[~partier['value'].isnull()]
    
    # 'value' är defaultnamnet från funktionen .melt()
    # och döps därför om till 'parti'
    partier.rename(columns={'value':'parti'},inplace=True)

    # Byt namn på FP till L
    partier.parti = partier.parti.str.replace('FP','L')
    
    # bort med ev mellanslag
    partier.parti=partier.parti.str.strip()
    
    # bort med alla understreck
    partier.parti=partier.parti.str.strip('_')
    
    # hämta valdata
    df = all_elec_years('K')
    
    # filtrera fram rätt år
    df=df.loc[df['valår']==year]
    
    
    
    # bort med onödiga rader med ogiltiga röster
    df=df.loc[~df['parti'].isin(slask)]
    
    # endast nödvändiga kolumner
    ph=df.loc[:,['kommun','parti','röster','mandat','procent']]
    
    # Sätt alla partier i stora bokstäver så matchning
    # blir korrekt
    ph.parti=ph.parti.str.upper()
    
    # lägg till valdata (mandat och procent) till de
    # styrande partierna
    partier=partier.merge(ph,
                          on=['kommun','parti'],
                          how='left')
    
    # lägg till blockkategorisering
    partier=partier.merge(styren.loc[:,['kommun',
                                        'block']],
                          on='kommun',
                          how='left')\
                    .sort_values('kommun')\
                    .reset_index(drop=True)
    
    # gruppera valdata efter kommun och block och återge
    ph = partier.groupby(['kommun','block']).sum().reset_index()
    
    ph=ph.merge(df.loc[:,['kommun','summa_mandat','summa_röster']].drop_duplicates(),on='kommun',how='left')

    ph=ph.loc[:,['kommun','summa_mandat','summa_röster']]
    
    partier=partier.merge(ph,on='kommun',how='left')
    
    partier.columns=pd.Series(partier.columns)\
                        .apply(lambda x: x + f'_{year}' \
                               if x in ['procent',
                                        'röster'
                                        'mandat',
                                        'majoritet',
                                        'summa_mandat',
                                        'summa_röster'] else x)
    
    return partier



def block_gov_count(df,\
                    value='mandat',\
                    parameter='minskat',\
                    elec_year='2018',\
                    compare_year='2014',\
                    research_data=None):
    """Räknar ut hur stor andel av alla blocksamarbeten som antingen
ökat eller minskat. Återger en lista där första tinget är en dataframe
på den formatterade data som beräkningarna bygger på (för kontroll).
Andra tinget är en jämförelse mellan de olika blockkonstellationerna.

PARAMETRAR
----------
df : Den blockdata som ska beräknas.

value : Strängvärde på vad man vill jämföra. Antingen 'mandat' eller \
'procent' (default).

parameter : Tar antingen strängvärdet 'minskat' eller 'ökat'. \
Default är 'minskning', dvs ifall inget strängvärde ges så kommer \
resultatet som visas vara hur stor andel av alla kommuner blocken \
styrde 2014 där de regerande partierna har minskat i stöd jämfört \
med 2014.

elec_year : ett årtal på den data man vill jämföra 2014 med. \
Resultatet blir den valdata som hämtas i mappen 'resultat'. Default \
är 2018."""
    
    #value = value + f'_{compare_year}'
    
    elec_year = str(elec_year)
    
    # lista över hur många partier som ingår i varje kommunstyre
    num_govs = df.kommun.value_counts().reset_index().rename(columns={'kommun':'num','index':'kommun'})
    
    # filtrering till en lista på de kommuner som har över ett parti i styret
    # för att endast beräkna på de kommunstyren som är partisamarbeten: 
    coop_govs=num_govs.loc[num_govs.num>1,'kommun']
    
    # filtrering av df:n på dessa kommuner
    df = df.loc[df.kommun.isin(coop_govs)]
    
    # hämtning av valdata att jämföra med:
    valdata = all_elec_years('K')
    
    valdata = valdata.loc[valdata['valår']==int(elec_year)]
    
    del valdata['valår']
    
    # dataformattering för att vara säker på att den importerade datan
    # är siffervärden och inte strängar:
    #valdata['procent'] = valdata['procent'].str.replace(',','.').astype('float')
    
    # Döper om kolumner:
    valdata.columns=pd.Series(valdata.columns)\
                        .apply(lambda x: x + f'_{elec_year}' \
                               if x in ['röster',
                                        'procent',
                                        'mandat',
                                        'summa_röster',
                                        'summa_mandat'] else x)
    
    # Lägger till den importerade valdatan till df:n
    df = df.merge(valdata, on=['kommun','parti'], how='left')
    #return df
    df.rename(columns={'procent':f'procent_{compare_year}',
                       'röster':f'röster_{compare_year}',
                       'mandat':f'mandat_{compare_year}',
                       'summa_mandat':f'summa_mandat_{compare_year}',
                       'summa_röster':f'summa_röster_{compare_year}'},inplace=True)

    if research_data:
        return df
    

    # Här summeras alla partier ihop till ett blockstyre per rad:
    muni_govs = df.loc[:,['kommun',
                   'block',
                   f'procent_{compare_year}',
                   f'mandat_{compare_year}',
                   f'procent_{elec_year}',
                   f'mandat_{elec_year}']]\
            .groupby(['kommun',
                      'block']).sum().reset_index()
    
    # Vi vill också se hur stor mandatminskningen är inom blocken
    # Här bryter vi ur den infon i en ny df som används längre ned
    df2 = muni_govs.groupby(['block']).sum()
    
    # Här räknar vi ut hur stor miskningen/ökningen är per blockstyre
    # i procent:
    df2 = (((df2[f'mandat_{elec_year}'] - df2[f'mandat_{compare_year}']) /\
            df2[f'mandat_{compare_year}'])*100).round(1).reset_index()
    
    df2 = df2.rename(columns={0:'total_mandatminskning_i_procent'})
    
    # uträkning på alla olika styrsamarbeten totalt:
    blockstyren = muni_govs.block.value_counts().reset_index()\
                    .rename(columns={'index':'block',
                                     'block':'antal_styren'})
    
    # uträkning på alla olika styrsamarbeten som har tappat i procent/mandat:
    if parameter == 'minskat':
        development = muni_govs.loc[(muni_govs[f'{value}_{elec_year}'] - \
                                     muni_govs[f'{value}_{compare_year}'])<0,'block']\
                        .value_counts().reset_index()\
                        .rename(columns={'index':'block',
                                         'block':'antal_styren_som_minskat'})
        
    elif parameter == 'ökat':
        development = muni_govs.loc[(muni_govs[f'{value}_{elec_year}'] - \
                                     muni_govs[f'{value}_{compare_year}'])>0,'block']\
                        .value_counts().reset_index()\
                        .rename(columns={'index':'block',
                                         'block':'antal_styren_som_ökat'})
    
    # här bildas en samlad lista 
    blockstyren=blockstyren.merge(development,on='block', how='left')
    
    # Här räknas det ut hur stor andel av respektive blockpartisamarbete
    # som har minskat i om det nya valresultatet. Det är en procentsiffra:
    blockstyren[f'andel_som_{parameter}'] = \
    ((blockstyren[f'antal_styren_som_{parameter}'] / \
      blockstyren.antal_styren)*100).round(1)
    
    # Lägger till mandatförändringen till slutreseultatet och transponerar
    # tabellen för bättre översikt:
    blockstyren = blockstyren.merge(df2,
                                    on='block',
                                    how='left')\
                             .set_index('block')
    
    blockstyren.rename(columns={'antal_styren':f'antal styren mandatperioden {compare_year}-{elec_year}',
                                'antal_styren_som_minskat':\
                                f'antal styren som {parameter} sitt stöd i valet {elec_year}',
                                'andel_som_minskat':f'andelen av styren som {parameter}',
                                'total_mandatminskning_i_procent':\
                                f'total mandat{parameter[:-2]+"ning"} i procent, {elec_year} jämfört {compare_year}'},
                      inplace=True)
    
    
    
    results = df.groupby(['kommun','block']).sum().reset_index().set_index('kommun')
    
    results = results.merge(valdata.loc[:,['kommun',
                                           f'summa_mandat_{elec_year}']]\
                                .drop_duplicates(),
                            on='kommun',
                            how='left')
    
    results[f'procentdiff_{compare_year}_{elec_year}'] = \
        (df[f'procent_{elec_year}']-\
         df[f'procent_{compare_year}']).round(1)
    
    results=results.merge(df.loc[:,['kommun',
                                     f'summa_mandat_{compare_year}']]\
                               .drop_duplicates(),
                           on='kommun',
                           how='left')
    
    results.loc[(results[f'mandat_{elec_year}']>\
                 (results[f'summa_mandat_{elec_year}']/2)),
                f'majoritet_{elec_year}'] = 'JA'
    results.loc[(results[f'mandat_{elec_year}']<\
                 (results[f'summa_mandat_{elec_year}']/2)),
                f'majoritet_{elec_year}'] = 'NEJ'
    
    results.loc[(results[f'mandat_{compare_year}']>\
                 (results[f'summa_mandat_{compare_year}']/2)),
                f'majoritet_{compare_year}'] = 'JA'
    results.loc[(results[f'mandat_{compare_year}']<\
                 (results[f'summa_mandat_{compare_year}']/2)),
                f'majoritet_{compare_year}'] = 'NEJ'
    
    results = results.loc[:,['kommun',
                         'block',
                         f'mandat_{compare_year}',
                         f'summa_mandat_{compare_year}',
                         f'majoritet_{compare_year}',
                         f'procent_{compare_year}',
                         f'mandat_{elec_year}',
                         f'summa_mandat_{elec_year}',
                         f'majoritet_{elec_year}',
                         f'procent_{elec_year}',
                         f'procentdiff_{compare_year}_{elec_year}']]
    
    _2014 = results.loc[results[f'majoritet_{compare_year}']=='JA','block']\
                .value_counts().reset_index()\
                .rename(columns={'index':'block',
                                 'block':f'antal_majoritetsstyren_{compare_year}'})

    _2018 = results.loc[results[f'majoritet_{elec_year}']=='JA','block']\
                .value_counts().reset_index()\
                .rename(columns={'index':'block',
                                 'block':f'antal_majoritetsstyren_{elec_year}'})
    
    styren = _2014.merge(_2018,
                         on='block',
                         how='left')
    
    blockstyren = blockstyren.reset_index()\
                        .merge(styren,
                               on='block',
                               how='left').set_index('block').T
    
    return [results,blockstyren]

def reshape_particip(df):
    df = df.loc[:,['kommun',
                   'kommunkod',
                   'summa_röster_fgval',
                   'valdeltagande_fgval']]
    
    df.rename(columns={
        'summa_röster_fgval':'summa_röster',
        'valdeltagande_fgval':'valdeltagande'
    },inplace=True)
    return df

def comma_remover(series):
    return series.str.replace(',','.').astype('float')



def all_mandates_2006(df):
    """Beräknar mandatsumma för 2006."""
    df1 = df.loc[df['valår']==2006]
    
    all_mandates = df1.groupby('kommun').sum().reset_index()\
                        .loc[:,['kommun',
                                'mandat']]\
                        .rename(columns={'mandat':'mandat_2006'})
    all_mandates['valår'] = 2006
    
    df=df.merge(all_mandates,on=['kommun','valår'],how='left')

    df.loc[df['valår']==2006,'summa_mandat'] = \
    df.loc[df['valår']==2006,'mandat_2006']

    del df['mandat_2006']
    
    return df
    #return df.merge(all_mandates,on='kommun',how='left')

def reshape(df):
    df = df.loc[:,['kommun',
                   'kommunkod',
                   'mandat_fgval',
                   'parti',
                   'procent_fgval',
                   'röster_fgval',
                   'valår']]
    
    df.rename(columns={
        'mandat_fgval':'mandat',
        'procent_fgval':'procent',
        'röster_fgval':'röster'
    },inplace=True)
    
    #df = all_mandates_2006(df)
    #print(df.summa_mandat.iloc[0])
    return df

def majority_calc(df, operator='mandat'):
    df = df.loc[df.parti != 'övriga_mindre_partier_totalt']
    df['summa_mandat'] = df['summa_mandat'].fillna(0).astype('int')
    
    mandat_totalt = df.loc[:,['kommun','valår','summa_mandat']].drop_duplicates()
    
    alliansen = ['M','C','L','KD']
    vänstern = ['S','V']
    
    slask = ['övriga_mindre_partier_totalt','OG','BLANK']

    df.loc[df['parti'].isin(vänstern),'block'] = 'V'

    df.loc[df['parti'].isin(alliansen),'block'] = 'A'

    df.loc[df['block'].isnull(),'block'] = 'Ö'
    
    df=df.groupby(['kommun','valår','block']).sum().reset_index()
    
    del df['summa_mandat']
    
    df=df.merge(mandat_totalt,on=['kommun','valår'],how='left')
    
    if operator == 'mandat':
        df.loc[df.mandat>(df.summa_mandat/2),'majoritet'] = 1

        df.loc[df.mandat<(df.summa_mandat/2),'majoritet'] = 0
    elif operator == 'procent':
        df.loc[df.procent>50,'majoritet'] = 1

        df.loc[df.procent<50,'majoritet'] = 0

        
    alliansen = df.loc[df['block']=='A'].pivot(index='kommun',columns='valår',values='majoritet')

    alliansen=alliansen.sum()

    vänstern = df.loc[df['block']=='V'].pivot(index='kommun',columns='valår',values='majoritet')

    vänstern=vänstern.sum()

    övriga = len(df.kommun.unique())-(vänstern+alliansen)
    #return df
    return pd.concat([alliansen,
               vänstern,
               övriga],axis=1).astype('int')\
        .rename(columns={0:'alliansen',
                         1:'vänstern',
                         2:'övriga'})

def all_particip_years(val):
    path_2010 = Path(f'data/meta_filer/\
valdeltagande/valdeltagande_2010{val}.xlsx')

    df = pd.DataFrame(columns=pd.read_excel(path_2010).columns)
    for year in ['2006','2010','2014','2018']:
        if year == '2006':
            data = pd.read_excel(path_2010)
            data = reshape_particip(data)
        else:
            path = Path(f'data/meta_filer/valdeltagande/valdeltagande_{year}{val}.xlsx')
            data = pd.read_excel(path)
    
        data['valår'] = int(year)
        df = pd.concat([df,data])
        
    return df.loc[:,['kommun','valår','summa_röster','valdeltagande','summa_mandat']]


def all_elec_years(val,exclude=True):
    """Denn funktion formatterar om alla grundfiler \
med respektive års valdata till en enhetlig och korrekt \
formatterad totallista för alla önskat val åren 2006-2018.

Funktionen använder sig också av följande funktioner:
- reshape(), tar bort och döper om kolumner
- comma_remover(), xml-datat plockar hem decimaler som strängar \
som pandas inte kan läsa om inte komman byts ut till punkter, \
vilket denna funktion fixar
- old_data_reshaper(), hämtar rätt siffror för år 2010 och 2014 \
(det kan nämligen vara så att somliga kommuner haft omval, denna \
funktion säkerställer att rätt jämförbart valresultat är med \
för alla kommuner).
"""
    path_2010 = Path(f'data/resultat/\
resultat_2010/valresultat_2010{val}.xlsx')
    df = pd.DataFrame(columns=pd.read_excel(path_2010).columns)
    for year in ['2006','2010','2014','2018']:
        if year == '2006':
            data = pd.read_excel(path_2010)
            data = reshape(data)
            data['procent'] = comma_remover(data['procent'])
        else:
            path = Path(f'data/resultat/resultat_{year}/valresultat_{year}{val}.xlsx')
            data = pd.read_excel(path)
            if (year == '2010') or (year == '2014'):
                data = old_data_reshaper(data,year,val)
            for col in ['procent','procent_fgval']:
                data[col] = comma_remover(data[col])
    
        data['valår'] = int(year)
        df = pd.concat([df,data])
        if exclude:
            df = df.loc[df['parti']!='övriga_mindre_partier_totalt']
        
    # slutligen, lägg till totala röster och mandat i kommunerna:
    munis_meta = all_particip_years(val)
    #munis_meta.valår = munis_meta.valår.astype('str')
    munis_meta = munis_meta.loc[:,['kommun','valår','summa_röster','summa_mandat']]

    #df.valår = df.valår.astype('str')
    df = df.merge(munis_meta, on=['kommun','valår'],how='left')
    
    df.valår = df.valår.astype('int')
    if val == 'K':
        df = all_mandates_2006(df)
    return df.loc[:,['kommun','valår','parti','röster','summa_röster','procent','mandat','summa_mandat']]

def old_data_reshaper(df,year,elec_type):
    """Den här funktionen byter ut valdata hämtade från \
alla grundfiler och byte ut dem med nästföljande vals \
valdata, då från kolumnen "[variabel]_fgval" - där "variabel" \
är mandat, procent och röster.
"""
    path = Path(f'data/resultat/\
resultat_{str(int(year)+4)}/valresultat_{str(int(year)+4)}{elec_type}.xlsx')
    new_data = pd.read_excel(path).loc[:,['kommun',
                                          'kommunkod',
                                          'parti',
                                          'mandat_fgval',
                                          'procent_fgval',
                                          'röster_fgval']]\
                                .rename(columns={
                                        'mandat_fgval':'mandat',
                                        'procent_fgval':'procent',
                                        'röster_fgval':'röster'})
    df = df.loc[:,['kommun','kommunkod','parti','mandat_fgval','procent_fgval','röster_fgval']]
    
    return df.merge(new_data,on=['kommun','kommunkod','parti'])
    
    

