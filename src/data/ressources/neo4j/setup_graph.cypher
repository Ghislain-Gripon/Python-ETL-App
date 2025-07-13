CALL () {
  CALL apoc.load.json("file:///nodes.json")
  YIELD value
  WHERE value.type = "drug"
  MERGE (d: Drugs {name: value.data})
};

CALL () {
  CALL apoc.load.json("file:///nodes.json")
  YIELD value
  WHERE value.type = "clinical_trials"
  MERGE (d: Clinical_Trials {title: value.data})
};

CALL () {
  CALL apoc.load.json("file:///nodes.json")
  YIELD value
  WHERE value.type = "pubmed"
  MERGE (d: Pubmed {title: value.data})
};

CALL () {
  CALL apoc.load.json("file:///nodes.json")
  YIELD value
  WHERE value.type = "journal"
  MERGE (d: Journal {name: value.data})
};

CALL () {
  CALL apoc.load.json("file:///edges.json")
  YIELD value
  WHERE value.source_type = "drug" AND value.target_type = "pubmed"
  MATCH (d: Drugs {name: value.source})
  MATCH (p: Pubmed {title: value.target})
  MERGE (d)-[:REFERENCE {date: date(datetime(value.date))}]->(p)
};

CALL () {
  CALL apoc.load.json("file:///edges.json")
  YIELD value
  WHERE value.source_type = "drug" AND value.target_type = "clinical_trials"
  MATCH (d: Drugs {name: value.source})
  MATCH (p: Clinical_Trials {title: value.target})
  MERGE (d)-[:REFERENCE {date: date(datetime(value.date))}]->(p)
};

CALL () {
  CALL apoc.load.json("file:///edges.json")
  YIELD value
  WHERE value.source_type = "clinical_trials" AND value.target_type = "journal"
  MATCH (c: Clinical_Trials {title: value.source})
  MATCH (j: Journal {name: value.target})
  MERGE (c)-[:MENTION {date: date(datetime(value.date))}]->(j)
};

CALL () {
  CALL apoc.load.json("file:///edges.json")
  YIELD value
  WHERE value.source_type = "pubmed" AND value.target_type = "journal"
  MATCH (p: Pubmed {title: value.source})
  MATCH (j: Journal {name: value.target})
  MERGE (p)-[:MENTION {date: date(datetime(value.date))}]->(j)
};

MATCH (d: Drugs)-[:REFERENCE]->(t:Clinical_Trials|Pubmed)-[m:MENTION]->(j: Journal)
MERGE (j)-[:MENTION {date: m.date}]->(d)