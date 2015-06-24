# Cassandra Model

The Cassandra Model gem aims at providing intuitive, simple data modelling capabilities for use with Ruby with Apache Cassandra, while still providing access to functionality that makes using Cassandra really powerful.

## Installation

As this project is currently in pre-version 1.0, it is only available through github. 

To integrate Cassandra Model in to your project, add the following lines to your application's Gemfile:

    gem 'thomas_utils', github: 'thomasrogers03/thomas_utils'
    gem 'cassandra_model', github: 'thomasrogers03/cassandra_model'

The Thomas Utils gem is a separate project (located at https://github.com/thomasrogers03/thomas_utils.git) used for some minor helper utitilities used within this project.

## Getting started

Cassandra Model offers a number of different ways to construct and uses models, from using existing tables created through your own migration framework, to building tables dynamically simply by describing how a table is intended to be used.

### A familiar starting point, for those of you are used to using ActiveRecord:

```ruby
require 'cassandra_model'

class Car < CassandraModel::Record
end

Car.create(make: 'Honda', year: 2014, model: 'Civic', colour: 'Green')
Car.create(make: 'Honda', year: 2013, model: 'Civic', colour: 'Blue')

recent_hondas = Car.where(make: 'Honda', :year.gt => 2.years.ago).get
```

This example illustrates how an existing **cars** table can be modelled to grab all the Honda vehicles whose year is at least 2 years ago. It assumes that you have an existing table **cars**, with a partition key *make* and a clustering column *year*.

### A more interesting example, demonstrating how to take advantage of asynchronous queries and token distribution in Cassandra:

```ruby
class Car < CassandraModel::Record
end

futures = []
futures << Car.create_async(make: 'Honda', year: 2014, model: 'Civic', colour: 'Green')
futures << Car.create_async(make: 'Honda', year: 2013, model: 'Civic', colour: 'Blue')
futures << Car.create_async(make: 'Toyota', year: 2014, model: 'Highlander', colour: 'Blue')
futures.map(&:join)

makes = %(Honda Toyota GM)
cars = makes.map { |make| Car.where(make: make, :year.gt => 2.years.ago).async }.map(&:get).flatten
```

This example shows how we can use Future/Promise API that the Datastax Cassandra ruby-driver provides to execute a number of queries asynchronously and wait for the results. 

More importantly, however, it shows us how we can take advantages of very fast writes across nodes in a Cassandra backed application. Such design principles quickly become very important when dealing with Cassandra in both write and read friendly requirements.

## Generating tables dynamically

There are two ways we can define meta tables (or tables that exist as we model them in Ruby). This can be done by defining a table definition, and then associating a MetaTable with the model, or by using built in data modelling features.

### Before we can create an meta tables

A table to keep track of all of these tables must be created 

```ruby
CassandraModel::TableDescriptor.create_descriptor_table
```

This method will create a table of descriptors for meta tables, if it does not already exist.

### Creating a MetaTable based model

```ruby
class Car < CassandraModel::Record
    TABLE_ATTRIBUTES = {
        name: :cars,
        partition_key: { make: :text },
        clustering_columns: { year: :int, model: :text },
        remaining_columns: { attributes: 'map<text, text>' }
    }
    TABLE_DEFINITION = CassandraModel::TableDefinition.new(TABLE_ATTRIBUTES)
    self.table = CassandraModel::MetaTable.new(TABLE_DEFINITION)
end
```

This model is now ready to be used similarly to the examples above, without having to write any CQL!

__Note__: Tables defined in this way will have unique identifiers appended to their name in Cassandra. This is done so we can modify the table design without dropping (!) and re-creating tables in Cassandra. The history of these tables is recorded in the **table_descriptors** table.

### Using data modelling helpers

The data modelling features in Cassandra Model are meant to be a somewhat verbose, high-level table designing tool to help build performant Cassandra tables with meaning. The following code demonstrates how we can re-create the **cars** table using this method.

```ruby
class Car < CassandraModel::Record
    extend CassandraModel::DataModelling

    model_data do |inquirer, data_set|
        inquirer.knows_about(:make)

        data_set.is_defined_by(:year, :model)
        data_set.change_type_of(:year).to(:int)
        data_set.knows_about(:colour)
    end
end
```

Here, we try to define the table in terms of what kind of questions we'd like to ask about our data. In this particular case, we can ask about all of the cars given a specific make, and learn about their details. Note that a columns by default are assumed to be text columns.

## Records with composite columns

One of the ways to avoid numerous queries to Cassandra is to de-normalize data as much a possible when saving records. However, sometimes we want to learn about data without having complete knowledge as to how it is defined. This can be accomplished using the CompositeRecord helpers. 

To use this, we define an inquirer and a data set with the pieces of information we know, and let it handle what we don't know.

```ruby
class Car < CassandraMode::Record
    extend CassandraModel::DataModelling

    model_data do |inquirer, data_set|
        inquirer.knows_about(:make, :model, :year, :colour)
        inquirer.knows_about(:make)
        inquirer.knows_about(:make, :model, :year)
        inquirer.knows_about(:vin)
        inquirer.defaults(:year).to(1900)

        data_set.is_defined_by(:price, :vin, :make, :model, :colour)
        data_set.change_type_of(:year).to(:int)
        data_set.knows_about(:description)
    end
end

Car.create(make: 'Honda', model: 'Civic', year: 2003, colour: 'Blue', vin: '123456789', description: 'A very reliable car')
```

With this model in hand, now we can ask questions like "What are all the cars we have for Toyota?" Or "How many 2001 Honda Civics do we have in blue?" We can also ask for the price range of a very specific model if we want to.

## Additional features

* Flexible table sharding
* Automatically rotating tables in use for maintenance
* Configuring multiple Cassandra connections over multiple keyspaces
* Mixing query helpers with ActiveRecord to build intuitive relations between a relational database and Cassandra

## Undocumented features

There are a number of features in Cassandra Model that may have missing or incomplete documentation. This will change as time progresses.

## Known issues

* As Cassandra Model uses splat arguments for providing query arguments, only Datastax ruby-driver versions up to 2.0.1 are supported.
* There is currently no elegant method of migrating data between different versions of meta tables
* CassandraModel::TableDescriptor.create_descriptor_table does not wait for table persistence in Cassandra
* CassandraModel::TableDescriptor.create_descriptor_table is vulnerable to being created multiple times when multiple applications are running with the same code


## Copyright

Copyright 2014-2015 Thomas Rogers.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.






