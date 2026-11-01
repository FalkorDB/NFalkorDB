using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Mapping;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

public class EntityMappingTests
{
    [Fact]
    public void Attributes_drive_the_label_and_property_keys()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.Equal(EntityKind.Node, metadata.Kind);
        Assert.Equal(new[] { "Person" }, metadata.Labels);
        Assert.Equal(":Person", metadata.LabelPattern);

        Assert.True(metadata.TryGetProperty(nameof(Person.Name), out var name));
        Assert.Equal("name", name.GraphName);
    }

    [Fact]
    public void Multiple_labels_are_preserved_in_order()
    {
        var metadata = EntityMetadataCache.Get<Company>();

        Assert.Equal(new[] { "Company", "Organization" }, metadata.Labels);
        Assert.Equal(":Company:Organization", metadata.LabelPattern);
    }

    [Fact]
    public void An_unannotated_type_falls_back_to_clr_names()
    {
        var metadata = EntityMetadataCache.Get<City>();

        Assert.Equal(new[] { "City" }, metadata.Labels);
        Assert.True(metadata.TryGetProperty(nameof(City.Population), out var population));
        Assert.Equal(nameof(City.Population), population.GraphName);
    }

    [Fact]
    public void A_relationship_type_is_mapped_by_its_type_name()
    {
        var metadata = EntityMetadataCache.Get<Knows>();

        Assert.Equal(EntityKind.Relationship, metadata.Kind);
        Assert.Equal("KNOWS", metadata.RelationshipType);
        Assert.Empty(metadata.Labels);
    }

    [Fact]
    public void The_graph_id_property_is_recorded_separately()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.NotNull(metadata.IdProperty);
        Assert.Equal(nameof(Person.Id), metadata.IdProperty.ClrName);
        Assert.True(metadata.IdProperty.IsId);

        Assert.DoesNotContain(metadata.Properties, p => p.IsId);
    }

    [Fact]
    public void An_ignored_property_is_not_mapped()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.False(metadata.TryGetProperty(nameof(Person.Transient), out _));
    }

    [Fact]
    public void Navigation_properties_are_recorded_with_their_target()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.True(metadata.TryGetNavigation(nameof(Person.Knows), out var knows));
        Assert.Equal("KNOWS", knows.RelationshipType);
        Assert.Equal(typeof(Person), knows.TargetType);
        Assert.Equal(TraversalDirection.Outgoing, knows.Direction);
        Assert.True(knows.IsCollection);

        Assert.True(metadata.TryGetNavigation(nameof(Person.Employers), out var employers));
        Assert.Equal(typeof(Company), employers.TargetType);
    }

    [Fact]
    public void A_navigation_property_is_not_also_a_stored_property()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.False(metadata.TryGetProperty(nameof(Person.Knows), out _));
    }

    [Fact]
    public void Metadata_is_cached_per_type()
    {
        Assert.Same(EntityMetadataCache.Get<Person>(), EntityMetadataCache.Get<Person>());
    }

    [Fact]
    public void Compiled_accessors_round_trip_a_value()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.True(metadata.TryGetProperty(nameof(Person.Age), out var age));

        var person = new Person();

        age.Setter(person, 42);

        Assert.Equal(42, person.Age);
        Assert.Equal(42, age.Getter(person));
    }

    [Fact]
    public void Array_properties_are_mapped_as_stored_values()
    {
        var metadata = EntityMetadataCache.Get<Person>();

        Assert.True(metadata.TryGetProperty(nameof(Person.Tags), out var tags));
        Assert.Equal("tags", tags.GraphName);
    }

    [Fact]
    public void A_type_with_no_mapped_properties_is_still_a_valid_label_only_node()
    {
        var metadata = EntityMetadataCache.Get<Marker>();

        Assert.Equal(new[] { nameof(Marker) }, metadata.Labels);
        Assert.Empty(metadata.Properties);
        Assert.Null(metadata.IdProperty);
    }

    [Fact]
    public void A_duplicate_graph_property_key_is_rejected()
    {
        var exception = Assert.Throws<GraphMappingException>(() => EntityMetadataCache.Get<DuplicateKeys>());

        Assert.Contains("name", exception.Message);
    }

    private class Marker
    {
    }

    [Node("Odd Label", "Plain")]
    private class AwkwardLabels
    {
        public int Age { get; set; }
    }

    [Fact]
    public void A_label_pattern_escapes_labels_that_are_not_bare_identifiers()
    {
        // LabelPattern is public and documented as a Cypher fragment, so it has to apply the same
        // escaping the MATCH renderer does rather than handing out a fragment that will not parse.
        Assert.Equal(":`Odd Label`:Plain", EntityMetadataCache.Get<AwkwardLabels>().LabelPattern);
    }

    private class DuplicateKeys
    {
        [Property("name")]
        public string First { get; set; }

        [Property("name")]
        public string Second { get; set; }
    }
}
