using System;
using System.Collections.Generic;
using NFalkorDB.Linq;

namespace NFalkorDB.Linq.Tests.Model;

public enum Rating
{
    Poor = 0,
    Good = 1,
    Great = 2
}

[Node("Person")]
public class Person
{
    [GraphId]
    public int Id { get; set; }

    [Property("name")]
    public string Name { get; set; }

    [Property("age")]
    public int Age { get; set; }

    [Property("height")]
    public double Height { get; set; }

    [Property("active")]
    public bool Active { get; set; }

    [Property("nickname")]
    public string Nickname { get; set; }

    [Property("score")]
    public int? Score { get; set; }

    [Property("rating")]
    public Rating Rating { get; set; }

    [Property("joined")]
    public DateTime Joined { get; set; }

    [Property("tags")]
    public string[] Tags { get; set; }

    [Relationship("KNOWS")]
    public List<Person> Knows { get; set; }

    [Relationship("WORKS_AT")]
    public List<Company> Employers { get; set; }

    [Ignore]
    public string Transient { get; set; }
}

[Node("Company", "Organization")]
public class Company
{
    [GraphId]
    public int Id { get; set; }

    [Property("name")]
    public string Name { get; set; }

    [Property("founded")]
    public int Founded { get; set; }
}

[Relationship("KNOWS")]
public class Knows
{
    [GraphId]
    public int Id { get; set; }

    [Property("since")]
    public int Since { get; set; }

    [Property("weight")]
    public double Weight { get; set; }
}

[Relationship("WORKS_AT")]
public class WorksAt
{
    [GraphId]
    public int Id { get; set; }

    [Property("role")]
    public string Role { get; set; }
}

/// <summary>
/// Unannotated on purpose: exercises the convention fallback (label and property keys come from
/// the CLR names).
/// </summary>
public class City
{
    public int Id { get; set; }

    public string Name { get; set; }

    public long Population { get; set; }
}
