/* Nlp-Hadoop
   Copyright (C) 2017 DISIT Lab http://www.disit.org - University of Florence

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>. */



NLP-HADOOP******************************************************************
L'applicazione java "NLP-Hadoop" permette l'esecuzione di una generica applicazione
GATE (file .xgapp) in un cluster distribuito Hadoop precedentemente installato
(testato con Hadoop versione 1.1.1).
L'applicazione come esempio esegue l'estrazione di keywords e keyphrase da
un file testo in ingresso (in italiano, ma è possibile configurare l'applicazione
anche per annotazioni file testo di lingua inglese).
****************************************************************************


PRE-REQUISIT DI INSTALLAZIONE:----------------------------------------------------

1) Apache Hadoop (http://hadoop.apache.org) installed on a multi-node cluster.
Tested on Hadoop Version 1.1.1, r1411108.
----------------------------------------------------------------------------------



CONTENUTO DEL PACKAGE:--------------------------------------------------------------------------
1) Cartella src -> contiene i sorgenti utilizzate per il corretto funzionamento 
		   della procedura MapReduce realizzata (Nlp-Hadoop).


2) Cartella jar -> contiene il jar compilato dell'applicazione Nlp-Hadoop eseguibile da HDFS.


3) Cartella lib -> contiene le librerie .jar necessarie al corretto funzionamento dell'applicazione.


4) Cartella Gate-App -> contiene lo zip dell'applicazione GATE che viene utilizzata 
		    da NLP-Hadoop.
------------------------------------------------------------------------------------------------