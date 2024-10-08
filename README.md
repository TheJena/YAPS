# YAPS
**Yet Another Provenance Suite**, formerly known as
[PROLIT](https://github.com/pasqualeleonardolazzaro/PROLIT), is a
Data Provenance for Data Science suite powered by LLMs.

This library aims at capturing fine-grained
[provenance](https://www.w3.org/TR/prov-dm), e.g. in a Data
Preparation real-world scenario, like often happening whenever
raw-data is pre-processed in order to produce machine learning-ready
datasets.  Built on top of Pandas, the _de-facto_ data science
standard library, YAPS provides a clear interface, without the need to
poison the source code with unneeded boilerplate additions; in that
sense LLMs are used to:

- automate the pipeline segmentation into _black-box_ operators
- document operators with descriptive summaries
- inject into the pipeline the very few library calls (e.g., to
  register and track operators)

After the pipeline execution, the provenance graph can be analysed via
Neo4j.

## Installation

To use the provenance suite, follow these steps:

1. Clone the repository:

   `git clone https://github.com/TheJena/YAPS.git ~/YAPS`

1. Create a virtual environment, e.g. with
   [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/install.html#basic-installation):

   ```
   mkvirtualenv -a ~/YAPS/                        \
                -r ~/YAPS/requirements.txt        \
                --reset-app-data                  \
                --verbose                         \
                --python=/usr/bin/python3         \
                --creator=venv                    \
                --download                        \
                YAPS
   ```

1. Navigate to the project directory and activate the virtual
   environment:

   `workon --cd YAPS`

1. Initialize secrets, replacing all the occurences found with:

   ```
   grep -HonrE "MY_[A-Z4_]+" . | fgrep -v "/.git/"

   # MY_NEO4J_USERNAME / MY_NEO4J_PASSWORD ~> Neo4j credentials
   # MY_NEO4J_DATA_DIR                     ~> where Neo4j will store graphs
   # MY_OLLAMA_DATA_DIR                    ~> where Ollama will store models
   ```

1. Start Neo4j and Ollama, e.g. via Docker Compose:

   `docker compose --project-directory ~/YAPS/neo4j up --detach`

1. Depending on the model size, you may would like to preload it in
   main memory

   _Optionally_, pull from the [library](https://ollama.com/library)
   the LLM of choice, e.g., Llama v3.1 with 70B parameters will use
   about 41GB of RAM

   `docker exec --interactive --tty ollama ollama pull llama3.1:70b`

1. Run the provided example:

   `python3 main.py  # -h/--help for more`

1. Access the Neo4j web interface
([http://localhost:7474/browser/](http://localhost:7474/browser/))
with a web browser and inspect the collected provenance

1. Stop Neo4j and Ollama, e.g. via Docker Compose

   `docker compose --project-directory ~/YAPS/neo4j down --volumes`

## Related Literature

This is a non exhaustive list of publications, for more updated works
please [query
scholar](https://scholar.google.com/scholar?as_ylo=2020&q=provenance+Missier+Torlone)

* Adriane Chapman, Luca Lauro, Paolo Missier and Riccardo
  Torlone. 2024. Supporting Better Insights of Data Science Pipelines
  with Fine-grained Provenance. ACM Trans. Database Syst. 49, 2,
  Article 6 (June 2024), 42 pages. [DOI:
  10.1145/3644385](https://doi.org/10.1145/3644385)

* Paolo Missier and Riccardo Torlone. From why-provenance to
  why+provenance: Towards addressing deep data explanations in
  Data-Centric AI. 32nd Symposium on Advanced Database Systems
  (SEBD2024), June 23-26, 2024, Villasimius, Sardinia, Italy, pp
  508-517. [URL:
  https://ceur-ws.org/Vol-3741/paper11.pdf](https://ceur-ws.org/Vol-3741/paper11.pdf)

* Luca Gregori, Paolo Missier, Matthew Stidolph, Riccardo Torlone and
  Alessandro Wood. Design and Development of a Provenance Capture
  Platform for Data Science. 2024 IEEE 40th International Conference
  on Data Engineering Workshops (ICDEW), Utrecht, Netherlands, May
  13-17, 2024, pp. 285-290. [DOI:
  10.1109/ICDEW61823.2024.00042](https://doi.org/10.1109/ICDEW61823.2024.00042)

* Adriane Chapman, Luca Lauro, Paolo Missier and Riccardo
  Torlone. 2022. DPDS: assisting data science with data
  provenance. Proc. VLDB Endow. 15, 12 (August 2022), 3614–3617. [DOI:
  10.14778/3554821.3554857](https://doi.org/10.14778/3554821.3554857)

* Adriane Chapman, Paolo Missier, Giulia Simonelli and Riccardo
  Torlone. Fine-grained provenance for high-quality data science. 29th
  Symposium on Advanced Database Systems (SEBD2021), September 5-9,
  2021, Pizzo Calabro, Calabria, Italy, pp 411-418. [URL:
  https://ceur-ws.org/Vol-2994/paper46.pdf](https://ceur-ws.org/Vol-2994/paper46.pdf)

* Adriane Chapman, Abhirami Sasikant, Giulia Simonelli, Paolo Missier
  and Riccardo Torlone. (2021). The Right (Provenance) Hammer for the
  Job: A Comparison of Data Provenance Instrumentation. In: Leslie
  F. Sikos, Oshani W. Seneviratne, Deborah L. McGuinness (eds)
  Provenance in Data Science. Advanced Information and Knowledge
  Processing. Springer, Cham. [DOI:
  10.1007/978-3-030-67681-0_3](https://doi.org/10.1007/978-3-030-67681-0_3)

* Adriane Chapman, Paolo Missier, Giulia Simonelli and Riccardo
  Torlone. 2020. Capturing and querying fine-grained provenance of
  preprocessing pipelines in data science. Proc. VLDB Endow. 14, 4
  (December 2020), 507–520. [DOI:
  10.14778/3436905.3436911](https://doi.org/10.14778/3436905.3436911)


## Credits
   - [Luca Lauro](https://github.com/LucaLauro) (UNIROMA3) which wrote
     the [very first
     prototype](https://github.com/LucaLauro/Data_Provenance1)
   - [Luca Gregori](https://github.com/Lucass97) (UNIROMA3) which
     extended and produced the [second
     prototype](https://github.com/Lucass97/data_provenance_for_data_science)
   - [Pasquale Leonardo
     Lazzaro](https://github.com/pasqualeleonardolazzaro) &
     [Marialaura Lazzaro](https://github.com/marialauraLazz)
     (UNIROMA3) which extended and produced the [third
     version](https://github.com/pasqualeleonardolazzaro/PROLIT) of
     the suite

## LICENSE

See full text license [here](COPYING); what follows are the copyright
and license notices.


```
Copyright (C) 2024      Federico Motta            <federico.motta@unimore.it>
                        Pasquale Leonardo Lazzaro <pas.lazzaro@stud.uniroma3.it>
                        Marialaura Lazzaro        <mar.lazzaro1@stud.uniroma3.it>
Copyright (C) 2022-2024 Luca Gregori              <luca.gregori@uniroma3.it>
Copyright (C) 2021-2022 Luca Lauro                <luca.lauro@uniroma3.it>

This file is part of YAPS, a provenance capturing suite

YAPS is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or any later
version.

YAPS is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with YAPS.  If not, see https://www.gnu.org/licenses/.
```
