namespace AcmeSearch;

public record AcmeDocsIndexModel
{
    required public int Id { get; init; }
    required public string sometextfield { get; init; }
    required public bool somebooleanfield { get; init; }
    required public int someintfield { get; init; }
    required public DateTime somedatefield { get; init; }
}