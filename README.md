# PyCon Philipinnes 2023

## Title

> "Introduction to Data Engineering: A Roadmap to Building Robust Data Infrastructures"
> "Data Engineering: A Roadmap to Building a Robust Data Infrastructure for Your Projects"


## Abstract

The benefits of having quality data available within an organization are plenty, but sadly, data isn't always (or almost) never in the exact shape and form in which data professionals need it. To address this gap between expectations and reality when it comes to having quality data, the role of the Data Engineer was born.

Data Engineers allow for greater visibility into a company's operations by enabling other data professionals to create internal and external tools that automate decisions (e.g. dashboards) and help bring in greater profits (e.g. recommendation systems). They do this by creating the data infrastructure within a company and orchestrating pipelines that move and transform data from different sources. Data touched by these pipelines often end up in data lake, a data warehouse, or it gets consumed via a streaming system.

That said, in this workshop you will put on the hat of a Data Engineer and walk through different examples that will help you gain sufficient knowledge to build a robust data platform for your projects using Python.


## Objectives

By the end of the workshop, you will be able to,
- Explain the role of a data engineer
- Gather the required information to build the data infrastructure for a project
- Create extract, transform, and load pipelines using Python and different open source libraries
- Orchestrate data pipelines based on their specific business logic

## Detailed Description

As soon as you walk into this workshop you will put on the hat of a Data Engineer and start walking through different examples that will help you gain sufficient knowledge to build a robust data platform for your projects using Python.

Data pipelines are useful tools for data professionals at all levels in an organisation and within different industries. From data analysts and scientists who want to move data around for their analyses and model-building and tunning stages, to data engineers building extract, transform, and load pipelines that scale the flow and accessibility of data within their organizations. With this in mind, the goal of this workshop is to help Python programmers learn how to build pipelines that move and transform data from different sources into others to then power analytical applications like dashboards, machine learning models, and chatbots, among many others..

The workshop will emphasize both methodology and frameworks through a top-down approach, and several of the open source libraries included are Prefect, Scikit-Learn, LightGBM, pandas, and the HoloViz suite of data visualization libraries. In addition, the workshop covers important concepts from data engineering, data analytics, and data science. Lastly, since we will start with the end results -- the overview of the system, the clean data we have, and the explanation of how we got it there -- participants will build a foundation for how to reverse engineer data pipelines and other processes they find in the wild by following the guidelines and processes provided in the workshop.


## Format
The tutorial has a setup section, three major lessons of ~50 minutes each, and 2 breaks of 10 minutes each. In addition, each of the lessons contains some allotted time for exercises that are designed to help solidify the content taught throughout the workshop.

## Audience
The target audience for this session includes analysts of all levels, developers, data scientists and machine learning engineers wanting to learn how to create different data pipelines and the appropriate infrastructure required for a data project.

### Prerequisites (P) and Good To Have's (GTH)

- **(P)** Attendees for this tutorial are expected to be familiar with Python (1 year of coding). 
- **(P)** Participants should be comfortable with loops, functions, lists comprehensions, and if-else statements.
- **(GTH)** While it is not necessary to have any knowledge of data analytics libraries, some experience with pandas, Prefect, matplotlib and scikit-learn, a bit of experience with these libraries would be very beneficial throughout this tutorial.
- **(P)** Participants should have at least 5 GB of free space in their computers.
- **(GTH)** While it is not required to have experience with integrated development environments like VS Code or Jupyter Lab, this would be very beneficial for the session.

## Outline

Total time budgeted (including breaks) - 3 hours

1. **Introduction and Setup (~10 minutes)**
	- Environment set up. An optional free-to-use environment will be provided in Binder, GitPod, Google Colab, and GitHub Codespaces
	- Agenda for the session
	- Instructor intro
	- Motivation for the workshop
	- Analytics vs Engineering
2. **Overview of a Data Platform (~40 minutes)**
	- Walk-through of the system
	- Breakdown of our Data Platform
	- Connecting data products to our platform
	- Extending our platform to cover machine learning use-cases
	- Exercise (7-min)
3. **10 minute break**
4. **Data Pipelines (~50 minutes)**
	- Intro to Data Pipelines
		- ETL vs ELT
		- Pipelines with `pandas
		- Pipelines with `Prefect
	- Understanding data versioning and lineage
	- Exercise (7-min).
5. **10 minute break**
6. **Infrastructure (~50 minutes)**
	- Orchestration
		- Monitoring and
		- scheduling pipelines
	- Testing our data platform
		- Testing code with `pytest`, `mypy`, and `hypothesis`
		- Testing Data with `great_expectations`
	- Automation
		- Writing a CI/CD pipeline
	- Exercise (7-min)
7. **Wrap Up**
	- Resources to continue your learning journey
	- Fun projects to go through on your own
	- Conclusion