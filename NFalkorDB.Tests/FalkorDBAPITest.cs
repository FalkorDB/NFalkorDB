using NFalkorDB.Tests.Utils;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;
using static NFalkorDB.Statistics;

namespace NFalkorDB.Tests;

public class FalkorDBAPITest : BaseTest
{
    private ConnectionMultiplexer _muxr;
    private Graph _api;
    private Graph _whatever;

    public FalkorDBAPITest() : base()
    {
    }

    protected override void BeforeTest()
    {
        _muxr = ConnectionMultiplexer.Connect(RedisConnectionString);

        _muxr.GetDatabase().Execute("FLUSHDB");

        _api = new FalkorDB(_muxr.GetDatabase(0)).SelectGraph("social");
        _whatever = new FalkorDB(_muxr.GetDatabase(0)).SelectGraph("whatever");
    }

    protected override void AfterTest()
    {
        _api = null;
        _muxr.Dispose();
        _muxr = null;
    }

    [Fact]
    public void TestCreateNode()
    {
        // Create a node    	
        ResultSet resultSet = _api.Query("CREATE ({name:'roi',age:32})");

        Assert.Equal(1, resultSet.Statistics.NodesCreated);
        Assert.Null(resultSet.Statistics.GetStringValue(Label.NodesDeleted));
        Assert.Null(resultSet.Statistics.GetStringValue(Label.RelationshipsCreated));
        Assert.Null(resultSet.Statistics.GetStringValue(Label.RelationshipsDeleted));
        Assert.Equal(2, resultSet.Statistics.PropertiesSet);
        Assert.NotNull(resultSet.Statistics.GetStringValue(Label.QueryInternalExecutionTime));

        Assert.Empty(resultSet);
    }

    [Fact]
    public void TestCreateLabeledNode()
    {
        // Create a node with a label
        ResultSet resultSet = _api.Query("CREATE (:human{name:'danny',age:12})");
        Assert.Empty(resultSet);
        Assert.Equal("1", resultSet.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Equal("2", resultSet.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.NotNull(resultSet.Statistics.GetStringValue(Label.QueryInternalExecutionTime));
    }

    [Fact]
    public void TestCreateLabeledNodeFireAndForget()
    {
        // Create a node with a label
        ResultSet resultSet = _api.Query("CREATE (:human{name:'danny',age:12})");
        Assert.Empty(resultSet);
        Assert.Equal("1", resultSet.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Equal("2", resultSet.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.NotNull(resultSet.Statistics.GetStringValue(Label.QueryInternalExecutionTime));
    }        

    [Fact]
    public void TestConnectNodes()
    {
        // Create both source and destination nodes
        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(_api.Query("CREATE (:person{name:'amit',age:30})"));

        // Connect source and destination nodes.
        ResultSet resultSet = _api.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)");

        Assert.Empty(resultSet);
        Assert.Null(resultSet.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(resultSet.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.Equal(1, resultSet.Statistics.RelationshipsCreated);
        Assert.Equal(0, resultSet.Statistics.RelationshipsDeleted);
        Assert.NotNull(resultSet.Statistics.GetStringValue(Label.QueryInternalExecutionTime));
    }

    [Fact]
    public void TestDeleteNodes()
    {
        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(_api.Query("CREATE (:person{name:'amit',age:30})"));
        ResultSet deleteResult = _api.Query("MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Assert.Empty(deleteResult);
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.RelationshipsCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.RelationshipsDeleted));
        Assert.Equal(1, deleteResult.Statistics.NodesDeleted);
        Assert.NotNull(deleteResult.Statistics.GetStringValue(Label.QueryInternalExecutionTime));

        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(_api.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));
        deleteResult = _api.Query("MATCH (a:person) WHERE (a.name = 'roi') DELETE a");

        Assert.Empty(deleteResult);
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.RelationshipsCreated));
        Assert.Equal(1, deleteResult.Statistics.RelationshipsDeleted);
        Assert.Equal(1, deleteResult.Statistics.NodesDeleted);

        Assert.NotNull(deleteResult.Statistics.GetStringValue(Label.QueryInternalExecutionTime));
    }

    [Fact]
    public void TestDeleteRelationship()
    {
        var graph = new FalkorDB(_muxr.GetDatabase(0)).SelectGraph($"social_{MethodBase.GetCurrentMethod()}");

        Assert.NotNull(graph.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(graph.Query("CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(graph.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(a)"));
        ResultSet deleteResult = graph.Query("MATCH (a:person)-[e]->() WHERE (a.name = 'roi') DELETE e");

        Assert.Empty(deleteResult);
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.PropertiesSet));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.RelationshipsCreated));
        Assert.Null(deleteResult.Statistics.GetStringValue(Label.NodesDeleted));
        Assert.Equal(1, deleteResult.Statistics.RelationshipsDeleted);

        Assert.NotNull(deleteResult.Statistics.GetStringValue(Label.QueryInternalExecutionTime));
    }

    [Fact]
    public void TestIndex()
    {
        // Create both source and destination nodes
        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));

        ResultSet createIndexResult = _api.Query("CREATE INDEX ON :person(age)");
        Assert.Empty(createIndexResult);
        Assert.Equal(1, createIndexResult.Statistics.IndicesCreated);

        // since RediSearch as index, those action are allowed
        ResultSet createNonExistingIndexResult = _api.Query("CREATE INDEX ON :person(age1)");
        Assert.Empty(createNonExistingIndexResult);
        Assert.NotNull(createNonExistingIndexResult.Statistics.GetStringValue(Label.IndicesCreated));
        Assert.Equal(1, createNonExistingIndexResult.Statistics.IndicesCreated);

        var exception = Assert.Throws<RedisServerException>(() => 
            _api.Query("CREATE INDEX ON :person(age)")
        );
        Assert.Contains("Attribute 'age' is already indexed", exception.Message);

        ResultSet deleteExistingIndexResult = _api.Query("DROP INDEX ON :person(age)");
        Assert.Empty(deleteExistingIndexResult);
        Assert.Equal(1, deleteExistingIndexResult.Statistics.IndicesDeleted);
    }

    [Fact]
    public void TestHeader()
    {
        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(_api.Query("CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(_api.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        ResultSet queryResult = _api.Query("MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age");

        Assert.NotNull(queryResult.Header);
        Header header = queryResult.Header;

        Assert.Equal("Header{schemaNames=[a, r, a.age]}", header.ToString());

        List<string> schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);

        Assert.Equal(3, schemaNames.Count);

        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Equal("a.age", schemaNames[2]);
    }

    [Fact]
    public void TestRecord()
    {
        string name = "roi";
        long age = 32;
        double doubleValue = 3.14;
        bool boolValue = true;

        string place = "TLV";
        long since = 2000;

        Property nameProperty = new Property("name", name);
        Property ageProperty = new Property("age", age);
        Property doubleProperty = new Property("doubleValue", doubleValue);
        Property trueBooleanProperty = new Property("boolValue", true);
        Property falseBooleanProperty = new Property("boolValue", false);
        Property nullProperty = new Property("nullValue", null);

        Property placeProperty = new Property("place", place);
        Property sinceProperty = new Property("since", since);

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.AddLabel("person");
        expectedNode.AddProperty(nameProperty);
        expectedNode.AddProperty(ageProperty);
        expectedNode.AddProperty(doubleProperty);
        expectedNode.AddProperty(trueBooleanProperty);

        Assert.Equal(
            "Node{labels=[person], id=0, propertyMap={name=Property{name='name', value=roi}, age=Property{name='age', value=32}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=True}}}",
            expectedNode.ToString());

        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";
        expectedEdge.AddProperty(placeProperty);
        expectedEdge.AddProperty(sinceProperty);
        expectedEdge.AddProperty(doubleProperty);
        expectedEdge.AddProperty(falseBooleanProperty);

        Assert.Equal(
            "Edge{relationshipType='knows', source=0, destination=1, id=0, propertyMap={place=Property{name='place', value=TLV}, since=Property{name='since', value=2000}, doubleValue=Property{name='doubleValue', value=3.14}, boolValue=Property{name='boolValue', value=False}}}",
            expectedEdge.ToString());

        var parms = new Dictionary<string, object>
        {
            {"name", name},
            {"age", age},
            {"boolValue", boolValue},
            {"doubleValue", doubleValue}
        };

        Assert.NotNull(_api.Query("CREATE (:person{name:$name,age:$age, doubleValue:$doubleValue, boolValue:$boolValue, nullValue:null})", parms));
        Assert.NotNull(_api.Query("CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(_api.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit') CREATE (a)-[:knows{place:'TLV', since:2000,doubleValue:3.14, boolValue:false, nullValue:null}]->(b)"));

        ResultSet resultSet = _api.Query("MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.name, a.age, a.doubleValue, a.boolValue, a.nullValue, r.place, r.since, r.doubleValue, r.boolValue, r.nullValue");

        Assert.NotNull(resultSet);

        Assert.Equal(0, resultSet.Statistics.NodesCreated);
        Assert.Equal(0, resultSet.Statistics.NodesDeleted);
        Assert.Equal(0, resultSet.Statistics.LabelsAdded);
        Assert.Equal(0, resultSet.Statistics.PropertiesSet);
        Assert.Equal(0, resultSet.Statistics.RelationshipsCreated);
        Assert.Equal(0, resultSet.Statistics.RelationshipsDeleted);
        Assert.NotNull(resultSet.Statistics.QueryInternalExecutionTime);
        Assert.Single(resultSet);

        Record record = resultSet.First();

        Node node = record.GetValue<Node>(0);
        Assert.NotNull(node);

        Assert.Equal(expectedNode, node);

        node = record.GetValue<Node>("a");

        Assert.Equal(expectedNode, node);

        Edge edge = record.GetValue<Edge>(1);
        Assert.NotNull(edge);
        Assert.Equal(expectedEdge, edge);

        edge = record.GetValue<Edge>("r");
        Assert.Equal(expectedEdge, edge);

        Assert.Equal(new[] {"a", "r", "a.name", "a.age", "a.doubleValue", "a.boolValue", "a.nullValue", "r.place", "r.since", "r.doubleValue", "r.boolValue", "r.nullValue"}, record.Keys);

        Assert.Equal([expectedNode, expectedEdge, name, age, doubleValue, true, null, place, since, doubleValue, false, null], record.Values);

        Assert.Equal("roi", record.GetString(2));
        Assert.Equal("32", record.GetString(3));
        Assert.Equal(32, record.GetValue<long>(3));
        Assert.Equal(32, record.GetValue<long>("a.age"));
        Assert.Equal("roi", record.GetString("a.name"));
        Assert.Equal("32", record.GetString("a.age"));
    }

    [Fact]
    public void TinyTestMultiThread()
    {
        ResultSet resultSet = _api.Query("CREATE ({name:'roi',age:32})");

        _api.Query("MATCH (a:person) RETURN a");

        for (int i = 0; i < 10000; i++)
        {
            var resultSets = Enumerable.Range(0, 16).AsParallel().Select(x => _api.Query("MATCH (a:person) RETURN a"));
        }
    }

    [Fact]
    public void TestMultiThread()
    {
        Assert.NotNull(_api.Query("CREATE (:person {name:'roi', age:32})-[:knows]->(:person {name:'amit',age:30}) "));

        List<ResultSet> resultSets = Enumerable.Range(0, 16).AsParallel().Select(x => _api.Query("MATCH (a:person)-[r:knows]->(b:person) RETURN a,r, a.age")).ToList();

        Property nameProperty = new Property("name", "roi");
        Property ageProperty = new Property("age", 32L);
        Property lastNameProperty = new Property("lastName", "a");

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.AddLabel("person");
        expectedNode.AddProperty(nameProperty);
        expectedNode.AddProperty(ageProperty);

        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";

        foreach (ResultSet resultSet in resultSets)
        {
            Assert.NotNull(resultSet.Header);
            Header header = resultSet.Header;
            List<String> schemaNames = header.SchemaNames;
            Assert.NotNull(schemaNames);
            Assert.Equal(3, schemaNames.Count);
            Assert.Equal("a", schemaNames[0]);
            Assert.Equal("r", schemaNames[1]);
            Assert.Equal("a.age", schemaNames[2]);
            Assert.Single(resultSet);
            Record record = resultSet.First();
            Assert.Equal(new[] {"a", "r", "a.age"}, record.Keys);
            Assert.Equal([expectedNode, expectedEdge, 32L], record.Values);
        }

        //test for update in local cache
        expectedNode.RemoveProperty("name");
        expectedNode.RemoveProperty("age");
        expectedNode.AddProperty(lastNameProperty);
        expectedNode.RemoveLabel("person");
        expectedNode.AddLabel("worker");
        expectedNode.Id = 2;

        expectedEdge.RelationshipType = "worksWith";
        expectedEdge.Source = 2;
        expectedEdge.Destination = 3;
        expectedEdge.Id = 1;

        Assert.NotNull(_api.Query("CREATE (:worker{lastName:'a'})"));
        Assert.NotNull(_api.Query("CREATE (:worker{lastName:'b'})"));
        Assert.NotNull(_api.Query("MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));

        resultSets = Enumerable.Range(0, 16).AsParallel().Select(x => _api.Query("MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r")).ToList();

        foreach (ResultSet resultSet in resultSets)
        {
            Assert.NotNull(resultSet.Header);
            Header header = resultSet.Header;
            List<String> schemaNames = header.SchemaNames;
            Assert.NotNull(schemaNames);
            Assert.Equal(2, schemaNames.Count);
            Assert.Equal("a", schemaNames[0]);
            Assert.Equal("r", schemaNames[1]);
            Assert.Single(resultSet);
            Record record = resultSet.First();
            Assert.Equal(new[] {"a", "r"}, record.Keys);
            Assert.Equal([expectedNode, expectedEdge], record.Values);
        }
    }

    [Fact]
    public void TestAdditionToProcedures()
    {
        Assert.NotNull(_api.Query("CREATE (:person{name:'roi',age:32})"));
        Assert.NotNull(_api.Query("CREATE (:person{name:'amit',age:30})"));
        Assert.NotNull(_api.Query("MATCH (a:person), (b:person) WHERE (a.name = 'roi' AND b.name='amit')  CREATE (a)-[:knows]->(b)"));

        List<ResultSet> resultSets = Enumerable.Range(0, 16).AsParallel().Select(x => _api.Query("MATCH (a:person)-[r:knows]->(b:person) RETURN a,r")).ToList();

        //expected objects init
        Property nameProperty = new Property("name", "roi");
        Property ageProperty = new Property("age", 32L);
        Property lastNameProperty = new Property("lastName", "a");

        Node expectedNode = new Node();
        expectedNode.Id = 0;
        expectedNode.AddLabel("person");
        expectedNode.AddProperty(nameProperty);
        expectedNode.AddProperty(ageProperty);

        Edge expectedEdge = new Edge();
        expectedEdge.Id = 0;
        expectedEdge.Source = 0;
        expectedEdge.Destination = 1;
        expectedEdge.RelationshipType = "knows";

        ResultSet resultSet = _api.Query("MATCH (a:person)-[r:knows]->(b:person) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        Header header = resultSet.Header;
        List<String> schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Single(resultSet);
        Record record = resultSet.First();
        Assert.Equal(new[] {"a", "r"}, record.Keys);
        Assert.Equal([expectedNode, expectedEdge], record.Values);

        //test for local cache updates

        expectedNode.RemoveProperty("name");
        expectedNode.RemoveProperty("age");
        expectedNode.AddProperty(lastNameProperty);
        expectedNode.RemoveLabel("person");
        expectedNode.AddLabel("worker");
        expectedNode.Id = 2;
        expectedEdge.RelationshipType = "worksWith";
        expectedEdge.Source = 2;
        expectedEdge.Destination = 3;
        expectedEdge.Id = 1;
        Assert.NotNull(_api.Query("CREATE (:worker{lastName:'a'})"));
        Assert.NotNull(_api.Query("CREATE (:worker{lastName:'b'})"));
        Assert.NotNull(_api.Query("MATCH (a:worker), (b:worker) WHERE (a.lastName = 'a' AND b.lastName='b')  CREATE (a)-[:worksWith]->(b)"));
        resultSet = _api.Query("MATCH (a:worker)-[r:worksWith]->(b:worker) RETURN a,r");
        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;
        schemaNames = header.SchemaNames;
        Assert.NotNull(schemaNames);
        Assert.Equal(2, schemaNames.Count);
        Assert.Equal("a", schemaNames[0]);
        Assert.Equal("r", schemaNames[1]);
        Assert.Single(resultSet);
        record = resultSet.First();
        Assert.Equal(new[] {"a", "r"}, record.Keys);
        Assert.Equal([expectedNode, expectedEdge], record.Values);
    }

    [Fact]
    public void TestEscapedQuery()
    {
        Assert.NotNull(_api.Query("MATCH (n) where n.s1='S\"\\'' RETURN n"));
        Assert.NotNull(_api.Query("MATCH (n) where n.s1='S\"\\'' RETURN n"));
    }

    [Fact]
    public void TestEscapedQueryAgain()
    {
        var params1 = new Dictionary<string, object>
        {
            { "s1", "S\"'" },
            { "s2", "S'\"" }
        };
        
        Assert.NotNull(_api.Query("CREATE (:escaped{s1:$s1,s2:$s2})", params1));

        var params2 = new Dictionary<string, object>
        {
            { "s1", "S\"'" },
            { "s2", "S'\"" }
        };

        Assert.NotNull(_api.Query("MATCH (n) where n.s1=$s1 and n.s2=$s2 RETURN n", params2));
    }

    [Theory]
    [MemberData(nameof(EscapedCypherParameters))]
    public void TestEscapedCypherParameters(Dictionary<string, object> parameters)
    {
        Assert.NotNull(_whatever.Query("CREATE (a:Test {SomeString: $SomeString})", parameters));
    }

    public static readonly object[][] EscapedCypherParameters =
    [
        new object[] {new Dictionary<string, object> {{"SomeString", "dsf\"dsfdss"}}},
        [new Dictionary<string, object> {{"SomeString", "dsfdsfdss\"#"}}],
    ];

    [Fact]
    public void TestArraySupport()
    {
        var expectedANode = new Node();
        expectedANode.Id = 0;
        expectedANode.AddLabel("person");
        var aNameProperty = new Property("name", "a");
        var aAgeProperty = new Property("age", 32L);
        var aListProperty = new Property("array", new object[] {0L, 1L, 2L});
        expectedANode.AddProperty(aNameProperty);
        expectedANode.AddProperty(aAgeProperty);
        expectedANode.AddProperty(aListProperty);

        var expectedBNode = new Node();
        expectedBNode.Id = 1;
        expectedBNode.AddLabel("person");
        var bNameProperty = new Property("name", "b");
        var bAgeProperty = new Property("age", 30L);
        var bListProperty = new Property("array", new object[] {3L, 4L, 5L});
        expectedBNode.AddProperty(bNameProperty);
        expectedBNode.AddProperty(bAgeProperty);
        expectedBNode.AddProperty(bListProperty);

        Assert.NotNull(_api.Query("CREATE (:person{name:'a',age:32,array:[0,1,2]})"));
        Assert.NotNull(_api.Query("CREATE (:person{name:'b',age:30,array:[3,4,5]})"));

        // test array

        var resultSet = _api.Query("WITH [0,1,2] as x return x");

        // check header
        Assert.NotNull(resultSet.Header);
        var header = resultSet.Header;

        var schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);

        Assert.Single(schemaNames);

        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Single(resultSet);
        var record = resultSet.First();
        Assert.Equal(new[] {"x"}, record.Keys);

        var x = record.GetValue<object[]>("x");
        Assert.Equal([0L, 1L, 2L], x);

        // test collect
        resultSet = _api.Query("MATCH(n) return collect(n) as x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);

        Assert.Single(schemaNames);

        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Single(resultSet);
        record = resultSet.First();
        Assert.Equal(new[] {"x"}, record.Keys);
        x = record.GetValue<object[]>("x");

        Assert.Contains(expectedANode, x);
        Assert.Contains(expectedBNode, x);

        // test unwind
        resultSet = _api.Query("unwind([0,1,2]) as x return x");

        Assert.NotNull(resultSet.Header);
        header = resultSet.Header;

        schemaNames = header.SchemaNames;

        Assert.NotNull(schemaNames);

        Assert.Single(schemaNames);

        Assert.Equal("x", schemaNames[0]);

        // check record
        Assert.Equal(3, resultSet.Count);

        for (var i = 0; i < 3; i++)
        {
            record = resultSet.ElementAt(i);

            Assert.Equal(new[] {"x"}, record.Keys);
            Assert.Equal(i, record.GetValue<long>("x"));
        }
    }

    [Fact]
    public void TestPath()
    {
        List<Node> nodes = new List<Node>(3);

        for (int i = 0; i < 3; i++)
        {
            var node = new Node();
            node.Id = i;
            node.AddLabel("L1");
            nodes.Add(node);
        }

        List<Edge> edges = new List<Edge>(2);

        for (int i = 0; i < 2; i++)
        {
            var edge = new Edge();
            edge.Id = i;
            edge.RelationshipType = "R1";
            edge.Source = i;
            edge.Destination = i + 1;

            edges.Add(edge);
        }

        var expectedPaths = new HashSet<Path>();

        var path01 = new PathBuilder(2).Append(nodes[0]).Append(edges[0]).Append(nodes[1]).Build();
        var path12 = new PathBuilder(2).Append(nodes[1]).Append(edges[1]).Append(nodes[2]).Build();
        var path02 = new PathBuilder(3).Append(nodes[0]).Append(edges[0]).Append(nodes[1]).Append(edges[1]).Append(nodes[2]).Build();

        expectedPaths.Add(path01);
        expectedPaths.Add(path12);
        expectedPaths.Add(path02);

        _api.Query("CREATE (:L1)-[:R1]->(:L1)-[:R1]->(:L1)");

        var resultSet = _api.Query("MATCH p = (:L1)-[:R1*]->(:L1) RETURN p");

        Assert.Equal(expectedPaths.Count, resultSet.Count);

        for (int i = 0; i < resultSet.Count; i++)
        {
            Path p = resultSet.ElementAt(i).GetValue<Path>("p");
            Assert.Contains(p, expectedPaths);
            expectedPaths.Remove(p);
        }
    }

    [Theory]
    [MemberData(nameof(TestParameterValues))]
    public void TestParameters(object parameters)
    {
        var param = new Dictionary<string, object>();

        object expected = parameters;
        param.Add("param", expected);
        ResultSet resultSet = _api.Query("RETURN $param", param);
        Assert.Single(resultSet);
        Record r = resultSet.First();
        object o = r.GetValue<object>(0);

        Assert.Equal(expected, o);
    }

    [Fact]
    public void TestParametersReadOnly()
    {
        // Dummy call to create the empty graph
        _api.Query("RETURN 1");

        var parameters = new object[]
        {
            1, 2.3, true, false, null, "str", 'a', "b", new List<int> {1, 2, 3},
            new[] {1, 2, 3}
        };

        var expected_anwsers = new object[]
        {
            1L, 2.3, true, false, null, "str", "a", "b", new List<long> {1L, 2L, 3L},
            new[] {1L, 2L, 3L}
        };

        var paramDict = new Dictionary<string, object>();

        for (int i = 0; i < parameters.Length; i++)
        {
            var param = parameters[i];
            paramDict["param"] = param;
            ResultSet resultSetRo = _api.ReadOnlyQuery("RETURN $param", paramDict);
            Assert.Single(resultSetRo);

            var oRo = resultSetRo.First().GetValue<object>(0);
            var expected = expected_anwsers[i];

            Assert.Equal(expected, oRo);
        }
    }

    [Fact]
    public void TestNullGraphEntities()
    {
        // Create two nodes connected by a single outgoing edge.
        Assert.NotNull(_api.Query("CREATE (:L)-[:E]->(:L2)"));

        // Test a query that produces 1 record with 3 null values.
        ResultSet resultSet = _api.Query("OPTIONAL MATCH (a:NONEXISTENT)-[e]->(b) RETURN a, e, b");
        Assert.Single(resultSet);
        Assert.Equal(new object[] {null, null, null}, resultSet.First().Values);

        // Test a query that produces 2 records, with 2 null values in the second.
        resultSet = _api.Query("MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY ID(a)");
        Assert.Equal(2, resultSet.Count);

        var record = resultSet.First();
        Assert.Equal(3, record.Values.Count);

        Assert.NotNull(record.Values[0]);
        Assert.NotNull(record.Values[1]);
        Assert.NotNull(record.Values[2]);

        record = resultSet.Skip(1).Take(1).First();
        Assert.Equal(3, record.Size);

        Assert.NotNull(record.Values[0]);
        Assert.Null(record.Values[1]);
        Assert.Null(record.Values[2]);

        // Test a query that produces 2 records, the first containing a path and the
        // second containing a null value.
        resultSet = _api.Query("MATCH (a) OPTIONAL MATCH p = (a)-[e]->(b) RETURN p");
        Assert.Equal(2, resultSet.Count);

        record = resultSet.First();
        Assert.Equal(1, record.Size);
        Assert.NotNull(record.Values[0]);

        record = resultSet.Skip(1).First();
        Assert.Equal(1, record.Size);
        Assert.Null(record.Values[0]);
    }

    [Fact]
    public void Test64BitNumber()
    {
        long value = 1L << 40;
        var parameters = new Dictionary<string, object>
        {
            { "val", value }
        };
        ResultSet resultSet = _api.Query("CREATE (n {val:$val}) RETURN n.val", parameters);

        Assert.Single(resultSet);

        Assert.Equal(value, resultSet.First().GetValue<long>(0));
    }

    [Fact]
    public void TestCachedExecution()
    {
        _api.Query("CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        var parameters = new Dictionary<string, object>
        {
            { "val", 1L }
        };

        var resultSet = _api.Query("MATCH (n:N {val:$val}) RETURN n.val", parameters);

        Assert.Single(resultSet);
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = _api.Query("MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }

        Assert.Single(resultSet);
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public void TestMapDataType()
    {
        var expected = new Dictionary<string, object>
        {
            { "a", (long)1 },
            { "b", "str" },
            { "c", null }
        };
        var d = new List<long>
        {
            1,
            2,
            3
        };
        expected.Add("d", d);
        expected.Add("e", true);
        var f = new Dictionary<string, object>
        {
            { "x", (long)1 },
            { "y", (long)2 }
        };
        expected.Add("f", f);
        ResultSet res = _api.Query("RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}");
        Assert.Single(res);
        // Record r = res.iterator().next();
        var something = res.First().Values[0];
        var actual = res.First().GetValue<Dictionary<string, object>>(0);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void TestGeoPointLatLon() {
        var rs = _api.Query("CREATE (:restaurant"
                                                   + " {location: point({latitude:30.27822306, longitude:-97.75134723})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);
    
        AssertTestGeoPoint();
    }
    
    [Fact]
    public void TestGeoPointLonLat() {
        var rs = _api.Query("CREATE (:restaurant"
                                                   + " {location: point({longitude:-97.75134723, latitude:30.27822306})})");
        Assert.Equal(1, rs.Statistics.NodesCreated);
        Assert.Equal(1, rs.Statistics.PropertiesSet);
    
        AssertTestGeoPoint();
    }
    
    private void AssertTestGeoPoint()
    {
        var results = _api.Query("MATCH (restaurant) RETURN restaurant");
        
        Assert.Single(results);
    
        var record = results.First();
        Assert.Equal(1, record.Size);
        Assert.Equal(new[]{"restaurant"}, record.Keys);
    
        var node = record.Values[0] as Node;
        var property = node?.PropertyMap["location"] ?? null;
        
        Assert.Equal(new Point(30.2782230377197, -97.751350402832), property.Value);
    }

    [Fact]
    public void TimeoutArgument() {
        var rs = _api.Query("UNWIND range(0,100) AS x WITH x AS x WHERE x = 100 RETURN x", timeout: 1L);
        
        Assert.Single(rs);
    
        var r = rs.First();
        
        Assert.Equal(100L, r.GetValue<long>(0));
    }

    [Fact]
    public void TestCachedExecutionReadOnly()
    {
        _api.Query("CREATE (:N {val:1}), (:N {val:2})");

        // First time should not be loaded from execution cache
        var parameters = new Dictionary<string, object>
        {
            { "val", 1L }
        };
        var resultSet = _api.ReadOnlyQuery("MATCH (n:N {val:$val}) RETURN n.val", parameters);

        Assert.Single(resultSet);
        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.False(resultSet.Statistics.CachedExecution);

        // Run in loop many times to make sure the query will be loaded
        // from cache at least once
        for (int i = 0; i < 64; i++)
        {
            resultSet = _api.ReadOnlyQuery("MATCH (n:N {val:$val}) RETURN n.val", parameters);
        }

        Assert.Single(resultSet);

        Assert.Equal(parameters["val"], resultSet.First().Values[0]);
        Assert.True(resultSet.Statistics.CachedExecution);
    }

    [Fact]
    public void TestSimpleReadOnly()
    {
        _api.Query("CREATE (:person{name:'filipe',age:30})");
        var rsRo = _api.ReadOnlyQuery("MATCH (a:person) WHERE (a.name = 'filipe') RETURN a.age");
        Assert.Single(rsRo);
        Assert.Equal(30L, rsRo.First().GetValue<long>(0));
    }

    public static object[][] TestParameterValues =
    [
        [1L],
        [2.3],
        [true],
        [false],
        [null],
        ["str"],
        [new List<long> {1, 2, 3}],
        [new[] {1L, 2L, 3L}],
        [new List<long> {1, 2, 3}.Select(n => new object[] {n, n.ToString()}).ToArray()],
        [new List<long> {1, 2, 3}.Select(n => new List<object> {n, n.ToString()}).ToList()]
    ];
}