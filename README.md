production_cycle_analyzer/
│
├── app/                      # UI layer (Tkinter)
│   ├── welcome_window.py
│   ├── main_window.py
│   └── results_window.py
│
├── core/                     # Domain (business logic)
│   ├── cycle_analyzer.py     # Algorithm to detect cycles
│   ├── models.py             # Data models (Cycle, TableResult)
│   └── interfaces.py         # Interfaces for infrastructure
│
├── services/                 # Application layer
│   └── analysis_service.py   # Orchestrates cycle analysis
│
├── infrastructure/           # Infrastructure
│   ├── repositories.py       # Table fetch/save
│   ├── database.py           # SQLAlchemy engine setup
│   ├── db_service.py         # Controls DB connections (SQLAlchemy)
│   ├── migration_service_fixed.py  
│   └── table_service.py  
│
├── main.py                   # Entry point (launch Tkinter app)
├── requirements.txt          # Libraries
└── README.md                 # Docs for GitHub

