using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using SolrNet.Exceptions;

namespace SolrNet.Impl {
    /// <summary>
    /// Maintains a collection of SolrConnections in case of node failure.
    /// </summary>
    public class SolrLoadBalancedConnection : ISolrConnection 
    {
        private readonly int numberOfRetries;
        private readonly int retryInterval;
        private readonly List<SolrConnectionWrapper> connectionsList = new List<SolrConnectionWrapper>(); 

        public SolrLoadBalancedConnection(IEnumerable<ISolrConnection> connections, int numberOfRetries, int retryInterval) 
        {
            this.numberOfRetries = numberOfRetries;
            this.retryInterval = retryInterval;

            foreach (var solrConnection in connections) 
            {
                this.connectionsList.Add(new SolrConnectionWrapper(solrConnection));
            }
        }

        public string Post(string relativeUrl, string s) {
            throw new System.NotImplementedException();
        }

        public string PostStream(string relativeUrl, string contentType, Stream content, IEnumerable<KeyValuePair<string, string>> getParameters) {
            throw new System.NotImplementedException();
        }

        public string Get(string relativeUrl, IEnumerable<KeyValuePair<string, string>> parameters) 
        {
            for (int i = 0; i < this.connectionsList.Count(); i++)
            {
                if (!this.connectionsList[i].IsActive)
                {
                    continue;
                }

                SolrConnectionException connectionException = null;

                for (int retry = 1; retry < this.numberOfRetries; retry++) 
                {
                    try 
                    {
                        return this.connectionsList[i].Connection.Get(relativeUrl, parameters);
                    } 
                    
                    catch (SolrConnectionException exception) 
                    {
                        connectionException = exception;
                        Thread.Sleep(this.retryInterval);
                    }
                }

                if (connectionException != null) 
                {
                    this.connectionsList[i].IsActive = false;    
                }
            }
 
            throw new SolrConnectionException("No active nodes found.");
        }

        private class SolrConnectionWrapper 
        {
            public ISolrConnection Connection { get; private set; }

            public bool IsActive { get; set; }

            public SolrConnectionWrapper(ISolrConnection connection) {
                Connection = connection;
                IsActive = true;
            }
        }
    }
}