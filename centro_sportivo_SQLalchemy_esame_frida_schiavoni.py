# Utilizzo della libreria SQLAlchemy: strumento Object Relational Mapper (ORM) per la comunicazione tra programmi Python e database.
# traduce le classi Python in tabelle su database relazionali e converte automaticamente le chiamate di funzione in istruzioni SQL


from datetime import *
from unicodedata import *

# metodo costruttore  create_engine crea Connessione al database
# Per usare DB-API per la connessione, la stringa ha la seguente forma:
# engine =  create_engine("dialect[+driver]://user:password@host/dbname",echo = True)
# driver :  mysql-connector-python

# create_engine() è il metodo costruttore crea una classe engine ovvero la  connessione al server MySQL
# ---- metodi classe Engine
# connect() Ritorna oggetto di connessione
# execute() Esegue un’istruzione SQL

from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://frida:basedati_2022@localhost/centro_sportivo')
conn = engine.connect()

# output delle info su connessione al database
print('')
print('CREATA CONNESSIONE CON DATABASE centro_sportivo: ' ,conn )
print("DRIVER di connessione con il DBMS: ",  engine.driver)
print("CONNESSIONE al Database : ", engine.url)
print('')
print("ELENCO TABELLE DATABASE centro_sportivo")
print('---------------------------------------')
from sqlalchemy import inspect
insp = inspect(engine)
result = insp.get_table_names()
for row in result: print(row)

# Metadata è un CATALOGO di oggetti Table e loro costrutti che contiene definizioni di tabelle (Table) e oggetti associati come indici, viste, trigger, ecc.
# ------------- i tipi generici ammessi in SQLAlchemy sono: --------------------------
# BigInteger  # SmallInteger   Integer  Numericb Floatn  Boolean String Text Date DateTime Time

# importazione oggetti per la definizione delle tabelle e degli indici all’interno del catalogo di metadati
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, Time, ForeignKey, Index, PrimaryKeyConstraint, ForeignKeyConstraint

# istanzia un oggetto MetaData
meta = MetaData()

# ----------- MAPPATURA DELLE TABELLE del database centro_sportivo sugli oggetti Python  -----------
tipo_manutenzione = Table('tipo_manutenzione', meta,
                          Column('CodIntervento', String, nullable=False, primary_key=True),
                          Column('Descrizione', String),
                          Column('Note', String),
                          )

tipo_mansioni = Table('tipo_mansioni', meta,
                      Column('CodTipoMansioni', String, nullable=False, primary_key=True),
                      Column('Descrizione', String),
                      )

mansioni = Table('mansioni', meta,
                 Column('CodMansione', String, nullable=False, primary_key=True),
                 Column('Descrizione', String),
                 Column('CodTipoMansioni', String, ForeignKey('tipo_mansioni.CodTipoMansioni')),
                 )

tesserini = Table('tesserini', meta,
                  Column('CodiceTesserino', String, primary_key=True, nullable=False),
                  Column('Stato', String),
                  Index('CodiceTesserino')
                  )

transiti = Table('transiti', meta,
                 Column('CodTesserino', String, ForeignKey('tesserini.CodiceTesserino'), nullable=False),
                 Column('Data', Date, nullable=False),
                 Column('Ora', Time, nullable=False),
                 Column('Verso', String, nullable=False),
                 PrimaryKeyConstraint('CodTesserino', 'Data', 'Ora', 'Verso'),  # chiave primaria composta,
                 Index('CodTesserino', 'Data', 'Ora'),
                 Index('Data')
                 )

anagrafica = Table('anagrafica', meta,
                   Column('CodiceFiscale', String(16), nullable=False, primary_key=True),
                   Column('Nominativo', String),
                   Column('DataNascita', Date),
                   Column('ComuneNascita', String),
                   Column('IndirizzoResidenza', String),
                   Column('Email', String, nullable=False),
                   Column('Telefono', String, nullable=False),
                   Index('CodiceFiscale')
                   )

clienti = Table('clienti', meta,
                Column('CodiceFiscale', String(16), ForeignKey('anagrafica.CodiCeFiscale'), nullable=False,
                       primary_key=True)
                )

assegnazione_tesserini = Table('assegnazione_tesserini', meta,
                               Column('CodTesserino', String, ForeignKey('tesserini.CodiceTesserino'), nullable=False),
                               Column('CodFiscale', String(16), ForeignKey('clienti.CodiceFiscale'), nullable=False),
                               Column('DataAssegnazione', Date),
                               PrimaryKeyConstraint('CodTesserino', 'CodFiscale'),  # chiave primaria composta,
                               ForeignKeyConstraint(['CodTesserino'], ['tesserini.CodiceTesserino'])
                               )

fasce_orarie = Table('fasce_orarie', meta,
                     Column('idFasciaOraria', String, nullable=False, primary_key=True),
                     Column('Descrizione', String, )
                     )

scaglioni_orari = Table('scaglioni_orari', meta,
                        Column('IdScaglioneOrario', String, nullable=False, primary_key=True),
                        Column('FasciaOrariaAppartenenza', String, ForeignKey('fasce_orarie.idFasciaOraria')),
                        Column('Descrizione', String, )
                        )

orario_lavoro = Table('orario_lavoro', meta,
                      Column('CodOrarioLavoro', String, nullable=False, primary_key=True),
                      Column('OraInizio',Time, nullable=False),
                      Column('OraFine', Time, nullable=False)
                      )

personale = Table('personale', meta,
                  Column('CodiceFiscale', String, ForeignKey('anagrafica.CodiCeFiscale'), nullable=False,
                         primary_key=True),
                  Column('CodOrarioLavoro', String, ForeignKey('orario_lavoro.CodOrarioLavoro')),
                  Column('CodMansione', String, ForeignKey('mansioni.CodMansione'), nullable=False),
                  Column('CodTesserino', String, ForeignKey('tesserini.CodiceTesserino'), nullable=False)
                  )

tipologia_corsi = Table('tipologia_corsi', meta,
                        Column('IdCorso', String, ForeignKey('personale.CodiCeFiscale'), nullable=False,
                               primary_key=True),
                        Column('Descrizione', String),
                        Column('Titolare', String, ForeignKey('Personale.CodFiscale'), nullable=False),
                        Column('IdSalaImpianto', String, ForeignKey('sale_impianti.IdSalaImpianto')),
                        Column('NumMin', Integer),
                        Column('NumMax', Integer)
                        )

orario_corsi_settimanali = Table('orario_corsi_settimanali', meta,
                                 Column('IdOrario', String, nullable=False, primary_key=True),
                                 Column('CodCorso', String, ForeignKey('tipologia_corsi.IdCorso'), nullable=False),
                                 Column('CodScaglioneOrario', String, ForeignKey('scaglioni_orari.IdScaglioneOrario')),
                                 Column('GiornoSettimana', String, nullable=False)
                                 )

prenotazioni_corsi = Table('prenotazioni_corsi', meta,
                           Column('IdOrario', String, ForeignKey('orario_corsi_settimanali.IdOrario'), nullable=False),
                           Column('Data', Date, nullable=False),
                           Column('CodFiscaleCliente', String, ForeignKey('clienti.CodiceFiscale'), nullable=False, ),
                           Column('Stato', String),
                           PrimaryKeyConstraint('IdOrario', 'Data', 'CodFiscaleCliente'),  # chiave primaria composta,
                           Index('IdOrario', 'Data', 'CodFiscaleCliente')
                           )

servizi_estetico_medici = Table('servizi_estetico_medici', meta,
                                Column('CodPrestazione', String, nullable=False, primary_key=True),
                                Column('Tariffa', Date, nullable=False),
                                Column('NumMin', Integer),
                                Column('NumMax', Integer)
                                )

orario_servizi = Table('orario_servizi', meta,
                       Column('IdOrario', String, nullable=False, primary_key=True),
                       Column('CodPrestazione', Date, ForeignKey('servizi_estetico_medici.CodPrestazione'), nullable=False),
                       Column('IdScaglioneOrario', String, ForeignKey('scaglioni_orari.IdScaglioneOrario')),
                       Column('GiornoSettimana', String, nullable=False)
                       )

prenotazioni_servizi = Table('prenotazioni_servizi', meta,
                             Column('IdOrario', String, ForeignKey('orario_servizi.IdOrario'), nullable=False),
                             Column('Data', Date, nullable=False),
                             Column('CodFiscaleCliente', String(16), ForeignKey('clienti.CodiceFiscale'), nullable=False),
                             Column('Stato', String),
                             PrimaryKeyConstraint('IdOrario', 'Data', 'CodFiscaleCliente'),
                             Index('IdOrario', 'Data', 'CodFiscaleCliente')
                             )

sale_impianti = Table('sale_impianti', meta,
                      Column('IdSalaImpianto', String, primary_key=True),
                      Column('Descrizione', String),
                      Column('Ubicazione', String),
                      Column('CapienzaMax', Integer),
                      )

orario_sale_impianti = Table('orario_sale_impianti', meta,
                             Column('IdOrario', String, primary_key=True),
                             Column('IdSalaImpianto', String, ForeignKey('sale_impianti.IdSalaImpianto')),
                             Column('ScaglioneOrario', String, ForeignKey('scaglioni_orari.IdScaglioneOrario')),
                             Column('GiornoSettimana', String)
                             )

prenotazioni_sale_impianti = Table('prenotazioni_sale_impianti', meta,
                                   Column('IdOrario', String, ForeignKey('orario_sale_impianti.IdOrario'), nullable=False),
                                   Column('Data', Date, nullable=False),
                                   Column('CodFiscaleCliente', String(16), ForeignKey('clienti.CodiceFiscale'),
                                          nullable=False),
                                   PrimaryKeyConstraint('IdOrario', 'Data', 'CodFiscaleCliente'),
                                   Index('IdOrario', 'Data', 'CodFiscaleCliente')
                                   )

tipo_abbonamenti = Table('tipo_abbonamenti', meta,
                         Column('IdAbbonamento', String, primary_key=True, nullable=False),
                         Column('Descrizione', String),
                         Column('Tariffa', Integer),
                         Column('Durata', Integer))

sconti = Table('sconti', meta,
               Column('CodSconto', String(10), nullable=False, primary_key=True),
               Column('Percentuale', Integer),
               Column('Descrizione', String))

abbonamenti = Table('abbonamenti', meta,
                    Column('ProgAbbonamento', Integer, primary_key=True, nullable=False, autoincrement=True),
                    Column('IdAbbonamento', String, ForeignKey('tipo_abbonamenti.IdAbbonamento')),
                    Column('CodFiscaleCliente', String(16), ForeignKey('clienti.CodiceFiscale')),
                    Column('CodSconto', String, ForeignKey('sconti.CodSconto')),
                    Column('DataInizio', Date)
                    )

manutenzione_impianti = Table('manutenzione_impianti', meta,
                              Column('CodIntervento', String, ForeignKey('tipo_manutenzione.CodIntervento'),nullable=False),
                              Column('CFManutentore', String(16), ForeignKey('personale.CodiceFiscale'), nullable=False),
                              Column('IdSalaImpianto', String, ForeignKey('sale_impianti.IdSalaImpianto'), nullable=False),
                              Column('Data', Date, nullable=False),
                              Column('OraInizio', Time, nullable=False),
                              Column('Durata', Integer),
                              PrimaryKeyConstraint('CodIntervento', 'CFManutentore', 'IdSalaImpianto', 'Data','OraInizio'),  # chiave primaria composta,
                              Index('Data', 'IdSalaImpianto'),
                              Index('CodIntervento')
                              )

# al metodo create_all() dell'oggetto meta - classe MetaData, si passa l’oggetto engine per creare le tabella e memorizzare tutte le informazioni in metadata
meta.create_all(engine)
from sqlalchemy.sql.functions import func

# -------------------------------------------------------------------------------
# ----------------- SELECT con classe sqlalchemy.sql ----------------------------
# -------------------------------------------------------------------------------

from sqlalchemy.sql import select, insert, delete, update, union
from sqlalchemy import and_, or_
from datetime import *

from datetime import *

print('')
print('---------------------------------------------  CONTROLLO ABBONAMENTI --------------------------------------- ')
print('1.	Selezionare gli estremi degli abbonamenti stipulati che abbiano durata annuale o sconto superiore al 15%')
print('')

# query 1
# SELECT  DATE_FORMAT( utc_date() , "%e-%c-%Y" ) as data_verifica_scadenze,
#                      DATE_FORMAT( abbonamenti.DataInizio , "%e-%c-%Y" ) as data_inizio,
#                      tipo_abbonamenti.Durata as durata_abbonamento,
#                      tipo_abbonamenti.Descrizione,
#                      sconti.Descrizione, anagrafica.Nominativo,
#                      anagrafica.Email, anagrafica.Telefono
#  FROM abbonamenti, anagrafica, tipo_abbonamenti, sconti
#  WHERE abbonamenti.CodFiscaleCliente = anagrafica.CodiceFiscale
#  AND  abbonamenti.IdAbbonamento = tipo_abbonamenti.IdAbbonamento
#  AND  abbonamenti.CodSconto = sconti.CodSconto
#  AND  (tipo_abbonamenti.Durata = 360 OR sconti.Percentuale >=15)

query1 = select( abbonamenti.c.DataInizio, tipo_abbonamenti.c.Durata, tipo_abbonamenti.c.Descrizione, sconti.c.Descrizione,
                 anagrafica.c.Nominativo, anagrafica.c.Email, anagrafica.c.Telefono).where(abbonamenti.c.CodFiscaleCliente==anagrafica.c.CodiceFiscale,
                                                                                           abbonamenti.c.IdAbbonamento==tipo_abbonamenti.c.IdAbbonamento,
                                                                                           abbonamenti.c.CodSconto == sconti.c.CodSconto,
                                                                                           or_(tipo_abbonamenti.c.Durata == 360, sconti.c.Percentuale >=15)

                                                                                           )
result = conn.execute(query1)
for row in result:
    print(row)


# QUERY 2
# SELECT anagrafica.Nominativo, transiti.CodTesserino,
#        DATE_FORMAT(transiti.Data, "%e-%c-%Y" ) as Data_Timbratura,  transiti.Ora,
#        REPLACE( REPLACE(transiti.Verso, 'I' ,'ENTRATA ') ,'U','USCITA ')  As Verso_timbratura,
#        orario_lavoro.*
# FROM personale, orario_lavoro, transiti , anagrafica
# WHERE transiti.CodTesserino = personale.CodTesserino
# AND orario_lavoro.CodOrarioLavoro= personale.CodOrarioLavoro
# AND anagrafica.CodiCeFiscale = personale.CodiCeFiscale
# ORDER BY personale.CodiCeFiscale, transiti.Data, transiti.Ora, transiti.Verso

print('')
print('---------------------------------------- CONTROLLO TRANSITI ----------------------------------------- ')
print('2.	Visualizzare elenco timbrature dei dipendenti ordinato per nominativo, data, ora e verso.')
print('')
query2 = select(  anagrafica.c.Nominativo, transiti.c.CodTesserino, transiti.c.Data, transiti.c.Ora, transiti.c.Verso,
                  orario_lavoro.c.CodOrarioLavoro, orario_lavoro.c.OraInizio, orario_lavoro.c.OraFine
               ).where( transiti.c.CodTesserino == personale.c.CodTesserino,
                        anagrafica.c.CodiceFiscale == personale.c.CodiceFiscale,
                        orario_lavoro.c.CodOrarioLavoro== personale.c.CodOrarioLavoro,
                        transiti.c.Data>= date(2022, 2, 2), transiti.c.Data<= date(2022, 5, 5),
               ).order_by( anagrafica.c.CodiceFiscale, transiti.c.Data,  transiti.c.Ora, transiti.c.Verso)

result = conn.execute(query2)
for row in result: print(row)


# QUERY 3
# SELECT  REPLACE(transiti.Verso, 'I' ,'ENTRATA ') As Verso_timbratura, anagrafica.Nominativo, orario_lavoro.CodOrarioLavoro, orario_lavoro.c.OraInizio,
#         DATE_FORMAT(transiti.Data, "%e-%c-%Y" ) as Data_Timbratura, MAX(transiti.Ora) AS CONTROLLO,
# FROM personale, orario_lavoro, transiti , anagrafica
# WHERE transiti.CodTesserino = personale.CodTesserino
# AND anagrafica.CodiCeFiscale = personale.CodiCeFiscale
# AND orario_lavoro.CodOrarioLavoro= personale.CodOrarioLavoro
# AND transiti.Verso ='I'
# UNION
#SELECT  REPLACE(transiti.Verso, 'U' ,'USCITA ') As Verso_timbratura, anagrafica.Nominativo, orario_lavoro.CodOrarioLavoro, orario_lavoro.c.OraFine,
#         DATE_FORMAT(transiti.Data, "%e-%c-%Y" ) as Data_Timbratura, MIN(transiti.Ora) AS CONTROLLO,
# FROM personale, orario_lavoro, transiti , anagrafica
# WHERE transiti.CodTesserino = personale.CodTesserino
# AND anagrafica.CodiCeFiscale = personale.CodiCeFiscale
# AND orario_lavoro.CodOrarioLavoro= personale.CodOrarioLavoro
# AND transiti.Verso ='U'

print('')
print('----------------------------------------  CONTROLLO scarto ora delle TIMBRATURE ---------------------------------------- ')
print('3.	Selezionare, per ciascun dipendente, il massimo ritardo di ingresso e la data in cui si è verificato, e, per ciascun dipendente,')
print('     la data e l’orario di uscita minimi, sempre in relazione al proprio orario di lavoro')
print('')

query3 = select(transiti.c.Verso, anagrafica.c.Nominativo, orario_lavoro.c.CodOrarioLavoro, orario_lavoro.c.OraInizio,  transiti.c.Data, func.max(transiti.c.Ora).label("controllo")
               ).where( anagrafica.c.CodiceFiscale == personale.c.CodiceFiscale,
                   transiti.c.CodTesserino == personale.c.CodTesserino,
                   transiti.c.Verso=='I',
                   orario_lavoro.c.CodOrarioLavoro== personale.c.CodOrarioLavoro
               ).group_by(anagrafica.c.Nominativo
               ).order_by(anagrafica.c.CodiceFiscale, transiti.c.Data).union\
        (
         select(transiti.c.Verso, anagrafica.c.Nominativo, orario_lavoro.c.CodOrarioLavoro, orario_lavoro.c.OraFine, transiti.c.Data, func.min(transiti.c.Ora).label("controllo")
            ).where(anagrafica.c.CodiceFiscale == personale.c.CodiceFiscale,
               transiti.c.CodTesserino == personale.c.CodTesserino,
                transiti.c.Verso == 'U',
                orario_lavoro.c.CodOrarioLavoro == personale.c.CodOrarioLavoro
            ).group_by(anagrafica.c.Nominativo
           ).order_by(anagrafica.c.CodiceFiscale, transiti.c.Data)
        )

result = conn.execute(query3)
for row in result: print(row)

# QUERY 4
# SELECT MAX(massimali.ore_lavorate) AS TotaleOre, massimali.codice as CodLavoro,
#            massimali.lavoro_eseguito as LavoroEseguito FROM
#           (  SELECT SUM(Durata) AS ore_lavorate,
#                     manutenzione_impianti.CodIntervento AS codice,
#                     tipo_manutenzione.Descrizione as lavoro_eseguito
#              FROM  manutenzione_impianti, tipo_manutenzione
#              WHERE tipo_manutenzione.CodIntervento =  manutenzione_impianti.CodIntervento
#              AND manutenzione_impianti.Data > "2020:04:18"
#              GROUP BY manutenzione_impianti.CodIntervento
#              ORDER BY manutenzione_impianti.CodIntervento  ) AS massimali
# 4.	Selezionare il codice e la descrizione della tipologia di manutenzione su sale ed impianti che ha
#       richiesto il massimo numero di lavoro a partire dal giorno 19 aprile 2022, indicando anche il monte ore raggiunto

print('')
print('-------------------------------   MASSIMALI ORE LAVORATE distinti per tipologie  ---------------------------------------- ')
print('4. Calcolo massimali ore lavoro:')
print('Selezionare il codice e la descrizione della tipologia di manutenzione su sale ed impianti raggruppate per numero di lavoro a partire dal giorno 19 aprile 2022')
print('indicando anche il massimo monte ore raggiunto.')
print('')
subq = select( func.sum(manutenzione_impianti.c.Durata).label("massimale"), (manutenzione_impianti.c.CodIntervento).label("codice"),
               (tipo_manutenzione.c.Descrizione).label("lavoro")
             ).where(manutenzione_impianti.c.CodIntervento == tipo_manutenzione.c.CodIntervento,
                     manutenzione_impianti.c.Data > date(2020, 4, 18)
             ).group_by(manutenzione_impianti.c.CodIntervento
             ).subquery()
result = conn.execute(subq)
for row in result: print(row)

query4 = select(func.max(subq.c.massimale))
print('')
print(' Massimale più alto -----------------------------------')
result = conn.execute(query4)
for row in result: print(row)

# query 5
# SELECT anagrafica.Nominativo, sale_impianti.Descrizione AS Impianto ,
# tipo_manutenzione.Descrizione, manutenzione_impianti.OraInizio,
# DATE_FORMAT(MAX(manutenzione_impianti.Data),"%e-%c-%Y" ) AS Ultima_data_manutenzione
# FROM  manutenzione_impianti, sale_impianti, tipo_manutenzione, anagrafica
# WHERE  manutenzione_impianti.CodIntervento = 'VASCA'
# AND manutenzione_impianti.CodIntervento=tipo_manutenzione.CodIntervento
# AND manutenzione_impianti.CFManutentore = anagrafica.CodiCeFiscale
# AND manutenzione_impianti.IdSalaImpianto = sale_impianti.IdSalaImpianto
# GROUP BY sale_impianti.IdSalaImpianto
print('')
print('----------------------------- CONTROLLO MANUTENZIONI ----------------------------------------------------------------')
print('5.	Controllare la data di ultima manutenzione delle vasche d’acqua, indicando l’ora e il nominativo del manutentore')
print('')
query5 = select( anagrafica.c.Nominativo, sale_impianti.c.Descrizione,tipo_manutenzione.c.Descrizione,
                 manutenzione_impianti.c.OraInizio, func.max(manutenzione_impianti.c.Data)
              ).where(  manutenzione_impianti.c.CodIntervento  == 'VASCA',
                        manutenzione_impianti.c.CodIntervento  == tipo_manutenzione.c.CodIntervento,
                        manutenzione_impianti.c.CFManutentore  == anagrafica.c.CodiceFiscale,
                        manutenzione_impianti.c.IdSalaImpianto == sale_impianti.c.IdSalaImpianto
              ).group_by(sale_impianti.c.IdSalaImpianto)

result = conn.execute(query5)
for row in result: print(row)

# Query 6
# SELECT   prenotazioni_corsi.Data AS DATA_PRENOTAZIONE, scaglioni_orari.Descrizione AS ORARIO_PRENOTAZIONE,
#          tipologia_corsi.Descrizione AS CORSO_PRENOTATO, prenotazioni_corsi.CodFiscaleCliente,
#          anagrafica.Nominativo AS CLIENTE,  orario_corsi_settimanali.CodCorso
# FROM prenotazioni_corsi, anagrafica, orario_corsi_settimanali,  tipologia_corsi, scaglioni_orari
# WHERE prenotazioni_corsi.CodFiscaleCliente = anagrafica.CodiCeFiscale
# AND prenotazioni_corsi.IdOrario = orario_corsi_settimanali.IdOrario
# AND orario_corsi_settimanali.CodCorso = tipologia_corsi.IdCorso
# AND orario_corsi_settimanali.CodScaglioneOrario = scaglioni_orari.IdScaglioneOrario
# AND prenotazioni_corsi.Data BETWEEN "2022-04-15" AND "2022-04-24"
# AND prenotazioni_corsi.Stato ='C'
# ORDER BY  prenotazioni_corsi.Data DESC , scaglioni_orari.Descrizione DESC , anagrafica.Nominativo
print('')
print('---------------------------------- CONTROLLO PRENOTAZIONI ATTIVE ----------------------------------------------------- ')
print('6.	Controllo delle prenotazioni attive e non disdette dal giorno 16 aprile 2022 al giorno 22 aprile 2022')
print('')
query6 = select( prenotazioni_corsi.c.Data, scaglioni_orari.c.Descrizione,
                 tipologia_corsi.c.Descrizione, prenotazioni_corsi.c.CodFiscaleCliente,
                 anagrafica.c.Nominativo,  orario_corsi_settimanali.c.CodCorso
               ).where( prenotazioni_corsi.c.CodFiscaleCliente == anagrafica.c.CodiceFiscale,
                        prenotazioni_corsi.c.IdOrario == orario_corsi_settimanali.c.IdOrario,
                        orario_corsi_settimanali.c.CodCorso == tipologia_corsi.c.IdCorso,
                        orario_corsi_settimanali.c.CodScaglioneOrario == scaglioni_orari.c.IdScaglioneOrario,
                        and_(prenotazioni_corsi.c.Data >= date(2022, 4, 16), prenotazioni_corsi.c.Data <= date(2022, 4, 24)),
                        prenotazioni_corsi.c.Stato =='C'
               ).order_by( prenotazioni_corsi.c.Data, scaglioni_orari.c.Descrizione, anagrafica.c.Nominativo)

result = conn.execute(query6)
for row in result: print(row)


# QUERY 7
# SELECT prenotazioni_corsi.IdOrario, count(prenotazioni_corsi.CodFiscaleCliente) AS NUMERO_PRENOTAZIONI,
#        orario_corsi_settimanali.GiornoSettimana, orario_corsi_settimanali.CodScaglioneOrario AS SCAGLIONE,
#        tipologia_corsi.Descrizione
# FROM prenotazioni_corsi,  orario_corsi_settimanali, tipologia_corsi
# WHERE prenotazioni_corsi.IdOrario = orario_corsi_settimanali.IdOrario
# AND orario_corsi_settimanali.CodCorso = tipologia_corsi.IdCorso
# AND prenotazioni_corsi.Stato ='C'
# GROUP BY (prenotazioni_corsi.IdOrario)
# HAVING orario_corsi_settimanali.CodScaglioneOrario IN
# ( SELECT scaglioni_orari.IdScaglioneOrario FROM scaglioni_orari WHERE scaglioni_orari.FasciaOrariaAppartenenza = "FASCIA1")
# sostituita subquery SQL con join .....
print('')
print('---------------------------------- PRENOTAZIONI FASCIA MATTUTINA ------------------------------------------')
print(' 7.  Numero di prenotazioni attive, distinte per tipologia di corso, nella fascia oraria mattutina')
print('')
query7 = select( prenotazioni_corsi.c.IdOrario, func.count(prenotazioni_corsi.c.CodFiscaleCliente),
                 orario_corsi_settimanali.c.GiornoSettimana, orario_corsi_settimanali.c.CodScaglioneOrario,tipologia_corsi.c.Descrizione
               ).where( prenotazioni_corsi.c.IdOrario == orario_corsi_settimanali.c.IdOrario,
                        orario_corsi_settimanali.c.CodCorso == tipologia_corsi.c.IdCorso,
                        prenotazioni_corsi.c.Stato =='C',
                        orario_corsi_settimanali.c.CodScaglioneOrario ==scaglioni_orari.c.IdScaglioneOrario,
                        scaglioni_orari.c.FasciaOrariaAppartenenza == "FASCIA1"
               ).group_by( prenotazioni_corsi.c.IdOrario)

result = conn.execute(query7)
for row in result: print(row)


# QUERY8
# 8. clienti che prenotano corsi di lunedì ma che nello stesso giorno non hanno mai prenotato sale o impianti.
# SELECT anagrafica.Nominativo
# FROM prenotazioni_corsi, anagrafica, orario_corsi_settimanali
# WHERE prenotazioni_corsi.CodFiscaleCliente = anagrafica.CodiCeFiscale
# AND prenotazioni_corsi.IdOrario = orario_corsi_settimanali.IdOrario
# AND orario_corsi_settimanali.GiornoSettimana = "Lunedì"
# GROUP BY anagrafica.Nominativo
# AND anagrafica.Nominativo <> ANY
# (SELECT anagrafica.Nominativo
# FROM prenotazioni_sale_impianti, anagrafica, orario_sale_impianti
# WHERE prenotazioni_sale_impianti.CodFiscaleCliente= anagrafica.CodiCeFiscale
# AND prenotazioni_sale_impianti.IdOrario = orario_sale_impianti.IdOrario
# AND orario_sale_impianti.GiornoSettimana = "Lunedì"
# Group BY anagrafica.Nominativo

print('')
print('---------------------------- CONTROLLO TIPOLOGIA PRENOTAZIONI ---------------------------------------------------------------------')
print('8.	Selezionare i nominativi dei clienti che prenotano corsi di lunedì ma che nello stesso giorno non hanno mai prenotato sale o impianti')
print('')
print( 'Clienti che prenotano corsi di lunedì  ---------------------------- ')
print('')
subquery1 = select( (anagrafica.c.Nominativo).label("nominativo")
                  ).where(        prenotazioni_corsi.c.CodFiscaleCliente == anagrafica.c.CodiceFiscale,
                               prenotazioni_corsi.c.IdOrario == orario_corsi_settimanali.c.IdOrario,
                               orario_corsi_settimanali.c.GiornoSettimana == "Lunedì"
                         ).distinct(anagrafica.c.Nominativo).subquery()
result = conn.execute(subquery1)
for row in result: print(row)

print('')
print( 'Clienti che prenotano sale ed impianti di lunedì  --------------------------------- ')
print('')
subquery2 = select( (anagrafica.c.Nominativo).label("nominativo")
               ).where( prenotazioni_sale_impianti.c.CodFiscaleCliente == anagrafica.c.CodiceFiscale,
                        prenotazioni_sale_impianti.c.IdOrario == orario_sale_impianti.c.IdOrario,
                        orario_sale_impianti.c.GiornoSettimana  == "Lunedì"
               ).distinct(anagrafica.c.Nominativo).subquery()

result = conn.execute(subquery2)
for row in result: print(row)

print('')
print('Clienti che di lunedì prenotano corsi ma non prenotano mai sale ed impianti --------------------------------- ')
print('IMPLEMENTAZIONE CON SUBQUERY :')
print('')
query8 = select(subquery1.c.nominativo).where(subquery1.c.nominativo != subquery2.c.nominativo)
result = conn.execute(query8)
for row in result: print(row)

print('')
print('Clienti che di lunedì prenotano corsi ma non prenotano mai sale ed impianti --------------------------------- ')
print('IMPLEMENTAZIONE PASSANDO AL COMPILATORE STRINGA DI TESTO CON STATMENT SQL :')
print('')
from sqlalchemy.sql import text
for row in result: print(row)
query8= text( "SELECT DISTINCT anagrafica.Nominativo FROM prenotazioni_corsi, anagrafica, orario_corsi_settimanali WHERE prenotazioni_corsi.CodFiscaleCliente = anagrafica.CodiCeFiscale AND prenotazioni_corsi.IdOrario = orario_corsi_settimanali.IdOrario AND orario_corsi_settimanali.GiornoSettimana = 'Lunedì' AND anagrafica.Nominativo <> ANY (SELECT DISTINCT anagrafica.Nominativo FROM prenotazioni_sale_impianti, anagrafica, orario_sale_impianti WHERE prenotazioni_sale_impianti.CodFiscaleCliente= anagrafica.CodiCeFiscale AND prenotazioni_sale_impianti.IdOrario = orario_sale_impianti.IdOrario AND orario_sale_impianti.GiornoSettimana = 'Lunedì') ")
result= conn.execute(query8)
for row in result: print(row)


# Query 9
# 9.	La segreteria necessita di mandare notifiche di scadenza al fine di invitare al rinnovo dell’abbonamento. Selezionare i nominativi,
# email e numero telefonico dei clienti del centro sportivo che hanno abbonamenti in scadenza (termine abbonamento a meno di 15  giorni)
print('----------------------------------------  ELENCO ABBONAMENTI IN SCADENZA--------------------------------------- ')
print('9. selezionare gli abbonamenti che scadono a meno di 15 giorni')
print('IMPLEMENTAZIONE PASSANDO AL COMPILATORE STRINGA DI TESTO CON STATMENT SQL :')
print('')
from sqlalchemy.sql import text

query9 = text('SELECT  DATE_FORMAT( utc_date() , "%e-%c-%Y" ) , DATE_FORMAT( abbonamenti.DataInizio , "%e-%c-%Y" ) ,  tipo_abbonamenti.Durata ,  DATE_FORMAT( (abbonamenti.DataInizio + INTERVAL tipo_abbonamenti.Durata DAY) , "%e-%c-%Y" )  , DATEDIFF( abbonamenti.DataInizio + INTERVAL tipo_abbonamenti.Durata DAY, CURDATE()) , tipo_abbonamenti.Descrizione, anagrafica.Nominativo, anagrafica.Email, anagrafica.Telefono  FROM abbonamenti, anagrafica, tipo_abbonamenti  WHERE abbonamenti.CodFiscaleCliente = anagrafica.CodiceFiscale  AND  tipo_abbonamenti.IdAbbonamento=abbonamenti.IdAbbonamento AND  DATEDIFF( abbonamenti.DataInizio + INTERVAL tipo_abbonamenti.Durata DAY, CURDATE()) < 25')

result = conn.execute(query9)
for row in result:
    print(row)




