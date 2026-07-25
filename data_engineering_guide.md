# DATA ENGINEERING GUIDE 

---

## CHAPTER 1: What is Data Engineering?

### SECTION 1: Why Data Engineering Exists

#### The Underlying Problem

Modern organisations produce data continuously and expansively. For example, a retail business records purchases, refunds, movements of stock, customer registrations, subscriptions, website activities, supplier deliveries, promotions, payments, and much more. A transport company produces vehicle locations, schedules, maintainance records, sales, weather observations, operational alerts. Furthermore, A bank produces transactions, account changes, credit decisions, fraud signal analytics, and regulatory records.

Data is unbiquitous in modern business operations, and is a core dependency of almost all business intelligence and initiatives. The difficulty with data is not merely that the data exists. The difficulty with data is that it usually:

- Is produced by many independent systems
- Stored in many different formats
- Produced as different speeds and intervals over time
- Governed by different business rules
- Incomplete, duplicated, late, or incorrect
- Too large for a single machine or application to viably process effectively
- Needed by many consumers for different disjoint purposes

Data engineering exists because raw operation data is seldomly immediately suitable for analytics, reporting, machine learning, regulatory application, or effective business decision making. 

A monetary transaction stored inside an order processing system may be perfectly adequate for satisfying a customer purchase. However, this does not mean that it is suitable for answering business intelligence related questions such as:

- Which product catagories are becoming less profitable?
- Which customer demographic is less likely to stop purchasing specific products?
- What proportion of orders were delivered late?
- How much inventory should each store hold for the next month?
- Can the company reporduce the figures used in the last quarter's regulatory report?

Real business intelligence (amongst other applications) require more than just a functional database doing it job. It warrants a system that collects, preserves, transforms, combines, validates, documents, secures, and delivers data reliably.

This is a core necessity that justifies data engineering.

---

#### Operational Systems Are Different From Analytical Systems

A core reason for the existance of data engineering is that systems designed to run a business are not automatically suitable for analysing that business.

Operational applications are designed around immediate actions such as: placing orders, updating an account, booking an appointment, recording a payment, changing inventory quantity, or authenticating a user.

These operational systems are predominantly optimised for fast reading and writing of data, transactional correctness, concurrent users, low response times, high avaliability, and the enforcement of application rules at the operational level.

On the contrary, analytical workloads are different to that of operational applications, often requiring the ability to scan transactions over a large historical timeframe, join data from many independent systems, aggregate billions of records, compare current and historical states, calculate metrics cross-departmentally, serve business intelligence dashboards or machine learning models.

The requirements of operational systems and analytical systems are distinct. Running the large analytical workloads directly against an operational database is poor data infrastructure design. A large analytical query could compete with the operational application system for memory, CPU, disk access, locks, or network capacity. Operational systems can technically run a large analytical query, however doing so may degrade the application that serves to keep the business operations fundamentally running.

Therefore, there is a fundamental individualisation considered, that separates operational from analytical systems. Operations systems serve the purpose of helping the organisation perform work. The analytical system help the organisation understand its work.

Data engineering designs, builds, and maintains the interconnecting bridge between the operational and analytical systems of an organisation.

---

#### Data Is Produced For One Purpose And Reused For Another

Data generally originates as a result of some operational process. 

For instance, a retailed stores an order because it must: charge the customer, reserve stock, arrange order fulfilment, calculate tax, communicate delivery information across departments.

The order is not necessarily stored in a structure that is suited for: customer segmentation, sales forecasting, supplier analytics, product performance reporting, fraud detection, or profitibility analysis.

The operational systems are generally the source of data, and reflects the instantaneous needs for continual operational work to remain consistent, efficient, and reliable. The analytical system must reflect the questions the organisation wants answers to.

A source application might store: customer records in a PostgreSQL database, payments in a third party API, website events in JSON, inventory snapshots in CSV files, product metadata in a document database, or support tickets in a SAAS platform.

Nevertheless, the organisation generally desires one trusted view encompassing its operations such as: revenue by customer, stock avaliability by location, order fulfilment performance, customer lifetime value, or promotional campaign effectiveness.

Data engineering reconciles these often fragmented representation into coherent and usable data products.

---

#### Data Fragmentation

Perfection in a system is a rarity. Organisations seldomly operate from a single unified and perfectly designed system. The tendency of organisations is to progressively accumulate more technologies over time, developing a technological ecosystem as needs expand or contract. 

Generally, organisations accumulate technology over time via: new product developments, business acquisitions, departmental independence, purchases from vendors, legacy systems, migrations to cloud platforms, temporary solutions that quietly became permanent, or different regulatory or geographic requirements.

Therefore, the accumulation of technological assets means the same entity may be represented differently across distinct systems.

For example, a customer may be identified in many different forms depending on the system and context it operates within. A customer could be represented with an ID number such as 182999 in a sales database. The same customer may have an account reference like UK-934523 in the support platform. They might be identified by email address in the marketing system. Moreover, the customer identification may be a payment provider token in the billing platform.

Consequently, entities tend to have labyrinthian qualities with no definitive foundation due to contraditory semantics between technologies used within an organisation. Even the most simple of attributes may be a source of conflicts: different spelling of names, different addresses, disaggrements with timestamps, different currencies, different account statuses, and different definitions of metrics, sementics, or the meaning of an entity internally within an organisation.

It is not just a technical inconvenience, but a semantics issue.

Before combining data for robust analytical workflows, data engineers / analytics engineers, and stakeholders must establish a foundational basis of agreed definitions.

Some things generally discussed include:

- Which records describe the same entity
- Which system is authoritative for each field
- How conflicts are to be resolved
- Whether historical data is to be preserved
- What business definitions apply
- How uncertainty should be represented

Data engineering therefore sits at the intersection of software system development and business domain knowledge.

---

#### Scale Changes The Engineering Problem


Scale refers to the size / volume of data being used by a system.

A small dataset can often by handled bery simply and manually with a script. For example, A data analyst might download a file, clean the data with Python using a dataframe manipulation library such as Pandas or Polars, and produce a report. This simple workflow is applicable when: The volume of data is small, the process is infrequent, only a single or small group depends on it, failure and its conseqeunces are significantly limited, and the data source is static, not changing over time.

This approach is common for beginners and data analysts working on a small scale. However, this approach becomes very fragile when: billions of rows of data needs processing (high volumes of data), data arrives continuously as a stream, hundreds of analytics dashboards depend on it, results must be avaliable before a deadline, historical records need to be reproducible, multitple teams / departments have involvement and require the data, the data contains sensitive information, and failure impacts customers or regulatory reporting.

Needless to say, the simple approach disintegrates in its viability proportionally to the scale, cross-departmental involvement, and overall complexity. The problem is not longer a simple dataset transformation, much more attention is needed for a reliable and effective system.

With augmented scale, the problems become:

- How should data be partitioned?
- Can processing be distributed?
- Can failed work be retried safely?
- How is duplicate processing prevented?
- How are late records handled?
- What happens when the underlying source data schema changes?
- Can the output be reproduced?
- How will downstream data consumers know when something is wrong?
- How much will the data pipeline cost?
- How are data access permissions handled?

Scale introduces very valid concerns with regards to computational power, storage capacities and methods, coordination, pipeline resilience, and cost.

With grander scale, distributed compute systems are typically incorporated as part of big data processing: cluster computing, distributed file systems, Apache Spark, Spark job scheduling, compute cluster management, containers, orchestration, monitoring, testing, and infrastructure as code, become more prevelant in development. 

These technologies and concepts are not the foundational definition of data engineering, although they are a paramount aspect of this technological discipline. These technologies are responses to engineering pressures introduced by augmentations in scale and complexity.

---

#### Automation As A Necessity

A data pipeline predominantly includes some form of automation. This automation typically is that of concerning the movement and transformation of data in the pipeline.

A simple data pipeline might include:

- Data extraction, such as extracting customer and order data from retailer databases
- Storage of raw data
- Cleaning of invalid fields
- Standardisation of data fields (such as timestamps)
- Removal of duplicate records
- Joining with other data tables (such as joining a customer table with an orders table)
- Calculating business intelligence metrics (such as daily revenue)
- Publishing the results to a reporting table

Automating this process is valuable, it saves time and resources by reducing unnecessary repetitions in manual data processing. However, an automated pipeline can still be unreliable.

An unreliable automated data pipeline might:

- Load the same records twice causing duplications
- Silently discard malformed rows without explicit quarantine or autit processes
- Calculate metrics such as revenue in the wrong currency
- Join tables at incompatible grains
- Publish incomplete data
- Use stale reference data
- Expose sensitive customer information
- produce different results when rerun

Therefore, another purpose of data engineering is not for automation. A more accurate description is: To create reliable systems that transform raw data into trustworthy, usable, and governed information.

I've chosen the words: reliable, trustworthy, usable, and governed very intentionally.

If the system is reliable, then the system behaves in alignment with expectations, and is robust. Meaning, it can handle failures, and recover.

If the system is trustworthy, then the output is deemed sufficiently correct for its purpose, complete, consistent, timely, and explainable for its intended usage.

If the system is usable, then the data is structured and documented in such a way, that consumer can actually understand it, and query it effectively.

If the system is governed, data access, ownership, lineage, retention, security, and regulatory compliance are controlled.

These aspects are all important. A data pipeline that moves data quickly, but produces misleading results is a failure of engineering design. That is not an effective data pipeline, but an automated mistake factory with disaterous consequences.

---

#### Repetition Legitemises Data Engineering Necessity

Within organisations, there is a superabundance of repetitive processes. Since organisations benefit from efficiency and being conservative with resources, reduction of repetition, and streamlining processes with automation, and engineering design, is highly valuable.

Suppose that a data analyst manually prepares a monthly revenue report.

The first time this report is made, the process of creating it may involve:

- Downloading a multitude of distinct spreadsheets
- Fixing data formats
- Removing cancelled orders
- Joining product data with other tables
- Calculating metrics
- Checking suspecious rows
- Sending the report to management 

If this is a one off ad-hoc report, this manual process is acceptable.

However, processes that happen frequently, automation and implementing robust data engineered systems is significantly more beneficial. Now, the organisation needs:

- Repeatable data extraction
- Formalised data transformation logic
- Data validation rules and contracts
- Pipeline orchestration and scheduling
- Pipeline monitoring and alerting
- Version controlled development
- Controlled authorisation of data access
- Extensive documentation
- Recovery procedures to protect from failures

This transition from one-off manual analytics, to repetable, robust, and dependable data systems is this clear distinction between data engineering, and other roles like data analysts.

The data engineer essentially transmutes tacit knowledge into an explicit system.

---

#### Data Engineering Reduces Organisational Uncertainty 

In the absence of reliable data systems, it is not uncommon for different departmental teams to produce different answers to the same question. For instance, a finance oriented team might define revenue using settled payments. The sales team may define it using signed orders. Operations teams may define it using fulfilled orders. Marketing may attribute it according to campaign conversions. 

Each individual answer to a question is internally consistent, while simultaneously conflicting with other external definitions of other departments.

This desynchronisation of definitions creates many disputes such as:

- Mismatching data reporting on analytics dashboards between difference departmental aspects of an organisation
- The dissolvement of trust in systems and between departments
- Confusion on definitions impacting the quality of reporting

Data engineering cannot resolve business ambiguity by its own virtue. However, it does provide the foundational mechanisms through which definitions can be implemented consistently.

Some of these mechanisms include:

- Consistent data models
- Governed transformation logic
- Conformed dimensions
- Semantic definitions
- Data validation contracts
- Lineage
- Version controlled production code
- Quality checks
- Documented ownership

An effectively designed data platform sigificantly reduces the number of hidden ambiguities and disharmonious interpretations lateltly existing around the organisation. Ground truth and trust is not magically or spontaneously created. Expicit definitions, that are inspectable and repetable form the bedrock of truth.

---

#### Data Engineering Enables Downstream Capabilities

The vast majority of modern data products depend on a plethora of enginerring effort that is invisuble and often underappreciated. Good data engineering allows aesthetically polished interfaces with data accomplished by front-end developers, while the back-end engineering provides the robust architecture that serves core functionality of a data product.

For instance, a business intelligence dashboards require: cleaned and modelled data, stable schemas, agreed metric definitions, acceptable refresh times, and reliable historical records.

A machine learning system requires: reproducible training data, feature consistency, historical snapshots, controlled authorisation of access, scalable processing, and model monitoring for drift and data quality failures.

An even more modern example is artificial intelligence systems. Generative AI and retrieval systems require: perfectly managed source documents, metadata, access controls, chuncking and indexing pipelines, data freshness management, evaluation datasets, and traceability.

The often invisible but monumentally important data engineering work also manifests in operational autonomous systems. Applications that use data to trigger other actions require: low letency in data delivery, reliable streamed events, idempotent processing, clear failure handling, and very strong data contracts.

Data engineering is not limited to just supporting an organisations technological infrastructure, but also plays a paramount role in regulation and auditing. Regulated reporting often requires: data lineage, reproducibility, historical preservation, access logs, controlled transformation, and explainable business rules.

The summation of this information demonstrate that data engineering often is the core discipline that enables technological infrastructure to work. The best data engineering may be completely invisible, and thats exactly the point. Well engineered data infrastructure means everything downstream simply works as intended. When data engineering is poor, every downstream discipline is impacted significantly as a consequence.

---

#### Data Engineering Constraints

The design and development of a data system is typically done so under many different tensions. There is no ubiquitously perfect data system, every decision tends to have some form of compromise or trade-off.

Typically an organisation wants data that is: perfectly accurate, instantaneously avaliable, inexpensive, indefinitely retained, accessible to everyone, completely secure, easy to modify, and stable for every downstream consumer. 

The desires of organisations are often lofty and idealistic, and conflict with themselves. For example:

- A lower latency data system usually requires more infrastructure and operational complexity, typically meaning more expenses
- Stronger data validation may delay publications
- Longer data retention increases storage cost and governance obligations
- Wider data accessibility increases security risks
- Highly flexible schemes tend to weaken consistency
- Strict data contracts slow rapid experimentation and timely modifications to infrastructure
- Maximising performance may reduce the maintainibility of a data system

Data engineering, like all engineering disciplines, are not about ambitiously searching for the universally perfect design. It is more often, the disciplined selection of trade-offs based on the operational requirements of the system.

To put this more explicitly, data engineering transmutes business requirements and constraints into technical systems and operational guarantees.

This means its important for a data engineer to apply technical and specific domain knowledge to answer questions such as:

- How fresh must data be for this system to satisfy the business requirements?
- What level of error is acceptable?
- How much data must be processed?
- How quickly does the volume of data grow?
- Who uses the outputs of this system?
- What happens when data arrives late?
- What happens if data arrives malformed or inaccurate?
- Can the processes in the system be rerun consistently without issue?
- How much is the organisation willing to spend?
- What data governance regulations apply?

Many more questions and important discussions occur in data engineering development scenarios. The most effective data architecture depends on answering questions and having clarity with a client organisation.

---

#### An Example Of Data Engineering

Consider a fictional retailer that operates: an e-commerce website, a multitude of physical stores, a warehouse inventory management system, a customer loyalty platform, a payment provider, and a marketing platform.

The management of this company wants a business intelligence dashboard showing key business metrics and KPIs such as: daily revenue, product performance, stoack avaliability, and customer behavioural analytics.

The company source data originates from a multitude of different channels: PostgreSQL order tables, store transaction files, payment API responses, product reference data, inventory snapshots, and customer events from their commerce website.

As typical for many organisations, there are many different technological subsystems making up thier operational ecosystem. These divergent systems disagree in several ways such as:

- Store timestamps for transactions use local time, while the online e-commerce store orders use UTC.
- Product identifiers changed after a product catalogue migration.
- Refunds arrive several data after the original sale.
- Some store files are uploaded late.
- A customer may appear under multiple identifiers.
- Payment status and order status are not equivalent.
- Website events contain duplicate messages.

These are some common problems for a retail organisation. Some of the data engineering work to address these issues include:

- Extracting or ingesting from each distinct origin of source data
- Preserving raw records in storage
- Attaching ingestion metadata
- Validating schemas
- Quarantining malformed data
- Standardisation of data fields such as timestamps and currencies
- Reconcile product and customer identifiers
- Deduplication of events
- Application of business rules for an agreed defintion of revenue
- Create fact and dimension tables
- Publish aggregated reporting tables
- Monitering completeness and data freshness
- Record data lineage
- Enforce access controls
- Observability of pipelines and alerting teams when expected data is missing.

This visible dashboard is only the final surface presentation. The deeper product is the engineered chain of evidence interconnecting source events to the final reported metrics.

---

#### Data Engineering Creates Confidence

Data engineering enabled entities such as tables are not valuable merely because it exists. The values comes from the intelligence it supports by design.

For instance, a finace director must be confident that revenue is calculated consistently. An operatioins manager must trust that stock information is fresh enough to effectively act upon. The data scientist must trust that the training data does not include future information causing temporal leakage. An auditor must trust in the ability to trace a reported figure through it components and to its source.

Therefore, data engineers not only produce the underlying data architectures that support most data intensive applications; but also, produce confidence through intelligence and intentional data engineering design choices. This confidence manifests as: repeatability, transparancy, validation, traceability, security, and operational reliability.

A key informal definition to take throughout the entirerity of this work, is the purpose of data engineering is to make data dependable enough to be used as evidence.

---

#### Some Key Engineering Principles

This chapter concludes the introduction to data engineering and its fundamental purpose. It is important to have insight and expansiveness in domain knowledge when conducting data engineering work. The questions, requirements, and constraints of a system form the core foundation of engineering design for data infrastructure. It is recommended, when examining any data system to ask yourself:

- Who is producing the data?
- Why was is originally produced?
- Who consumes it?
- What decisions and functionality depends on it?
- What does a singular record represent?
- How fresh must data be?
- What could make data wrong?
- What happens when processing fails?
- Can results be reproduced?
- Who owns its definitions and quality?

Questions such as these cut through the often dense technical layers present in data engineering projects and literature, and reveals the real value in design: the underlying context and circumstances that educated certain implementation choices and the justifications thereof.

The understanding of contextual design, technical compromises, and looking at data systems holistically and purposefully is what separates a junior data engineer from a senior. Real value in data engineering is partially having technical knowledge to establish a system; However, what is more powerful is knowing what to implement, rather than exclusively how to implement a solution.

In conclusion, throughout this chapter we have dicussed what data engineering is and its definition. They basic description may say: data engineering is the process of data collection, transformation, and storage. This is not incorrect; However, it is superficial and lacks clarity. The key distinction is in the more formal definition I propose: data engineering is the discipline of designing, building, operating, and governing systems that handle the movement, transformation, storage, and serving of trustworthy data reliably for both analytical and operational use.

The stronger definition proposed includes: design, implementation, operation, reliability, governance, consumers, and intentional use. These are the elements the create the distinction between data processing and fully fledged data engineering practice.

---