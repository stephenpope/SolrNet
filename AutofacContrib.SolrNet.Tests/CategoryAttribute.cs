using Xunit;

namespace AutofacContrib.SolrNet.Tests
{
    public class CategoryAttribute : TraitAttribute
    {
        public CategoryAttribute(string category) : base("Category", category) { }
    }
}