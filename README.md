# PyCon Philipinnes 2023

## Title

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

The workshop will emphasize both methodology and frameworks through a top-down approach, and several of the open source libraries included are pandas and metaflow. In addition, the workshop covers important concepts from data engineering, data analytics, and data science. Lastly, since we will start with the end results -- the overview of the system, the clean data we have, and the explanation of how we got it there -- participants will build a foundation for how to reverse engineer data pipelines and other processes they find in the wild by following the guidelines and processes provided in the workshop.


## Format
The tutorial has a setup section, three major lessons of ~50 minutes each, and 2 breaks of 10 minutes each. In addition, each of the lessons contains some allotted time for exercises that are designed to help solidify the content taught throughout the workshop.

## Audience
The target audience for this session includes analysts of all levels, developers, data scientists and machine learning engineers wanting to learn how to create different data pipelines and the appropriate infrastructure required for a data project.

### Prerequisites (P) and Good To Have's (GTH)

- **(P)** Attendees for this tutorial are expected to be familiar with Python (1 year of coding). 
- **(P)** Participants should be comfortable with loops, functions, lists comprehensions, and if-else statements.
- **(GTH)** While it is not necessary to have any knowledge of data analytics libraries, some experience with pandas would be very beneficial throughout this tutorial.
- **(P)** Participants should have at least 5 GB of free space in their computers.
- **(GTH)** While it is not required to have experience with integrated development environments like VS Code or Jupyter Lab, this would be very beneficial for the session.

## Outline

Total time budgeted (including breaks) - 3 hours

1. **Introduction and Setup (~20 minutes)**
	- Environment set up. An optional free-to-use environment will be provided in Binder, GitPod, Google Colab, and GitHub Codespaces
	- Instructor intro
	- Motivation for the workshop
	- Agenda for Today
		1. What is Data Engineering?
			- A quick note on Analytics Engineering vs Data Engineering
		2. Core Tenets of a Data Architecture
		3. Creating Own Data Orchestration Tool
			- Intro to Data Pipelines
		4. A Framework Solution
			1. Workflow
			2. Tools
		5. A Platform Solution
		6. Next Steps
	- Kick Off
2. **Data Engineering From Scratch (~1 hour)**
	- Walk-through of the system
	- Intro to Data Pipelines
	- Creating a Framework
	- Bulding ETL command line tools
	- Exercise (7-min)
3. **10 minute break**
4. **Data Infrastructure (~1 hour)**
	- What our previous solution lack
	- Automating Data Pipelines
	- Understanding data versioning and lineage
	- Exercise (7-min).
5. **10 minute break**
6. **Data Platform Demo (~10 minutes)**
7. **Wrap Up**
8. **Resources**


## Setup

If you are on a Mac or Linux machine, the set up steps will be the same. If you are in a Windows machine, you can still follow along with the tutorial, but please bare in mind that your experience  will be a much better one if you were to download, install and use (preferrably) Windows Subsystem for Linux or Git Bash for the session.

You can set up the environment with conda or with virtualenv. For those who want to use conda, you should first make sure you have, [Miniconda](https://docs.conda.io/en/latest/miniconda.html) installed before following the next steps.

You will also need to have docker installed and set up in your computer. While we won't be using it, part of the tutorial depends on it.

#### First Step

Open up your terminal and navigate to a directory of your choosing in your computer. Once there, run the following command to get the code for the session.

```sh
 git clone https://github.com/ramonpzg/pycond-ph-deng-23.git
```

Conversely, you can click on the green `download` button at the top and donwload all files to your desired folder/directory. Once you download it, unzip it and move on to the second step.

#### Second Step

To get all dependencies, packages and everything else that would be useful in this tutorial, you can recreate the environment by first going into the directory for today.

```sh
cd pycon-ph_pycon23
```

Then you will need to create an environment with all of the dependancies needed for the session by running the following command.

```sh
conda env create -f environment.yml
conda activate phcon

## OR

python -m venv venv
source venv/bin/activate
pip install -f requirements.txt
```

#### Third Step

Open up Jupyter Lab and you should be ready to go.

```sh
jupyter lab
```

Great work! Now navigate to `notebooks/01_from_scratch.ipynb` and open it.


## Resources

Here are a few great resources to get started with the topics covered in this workshop.

- [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) by Joe Reis and Matt Housley
- [Fundamentals of Data Visualisation](https://clauswilke.com/dataviz/) by Claus O. Wilke
- [Python for DevOps](https://www.amazon.com/Python-DevOps-Ruthlessly-Effective-Automation/dp/149205769X) by Noah Gift, Kennedy Behrman, Alfredo Deza, and Grig Gheorghi
- [Python for Data Analysis: Data Wrangling with Pandas, NumPy, and IPython](https://wesmckinney.com/book/) by Wes McKinney