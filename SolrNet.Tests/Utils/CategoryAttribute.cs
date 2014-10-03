using Xunit;

namespace SolrNet.Tests.Utils
{
    public class CategoryAttribute : TraitAttribute
    {
        public CategoryAttribute(string category) : base("Category", category) { }
    }
}