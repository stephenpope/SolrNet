using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using SolrNet.Exceptions;

namespace SolrNet.Impl 
{
    /// <summary>
    /// Maintains a collection of SolrConnections in case of node failure.
    /// </summary>
    public class SolrLoadBalancedConnection : ISolrConnection 
    {
        private readonly int numberOfRetries;
        private readonly int retryInterval;
        private readonly bool retryDeadConnections;
        private readonly int deadConnectionRetryTime;

        // TODO: .NET 4.0 : ConcurrentList !!
        private readonly List<SolrConnectionWrapper> connectionsList = new List<SolrConnectionWrapper>(); 

        public SolrLoadBalancedConnection(IEnumerable<ISolrConnection> connections, int numberOfRetries, int retryInterval, bool retryDeadConnections, int deadConnectionRetryTime) 
        {
            this.numberOfRetries = numberOfRetries;
            this.retryInterval = retryInterval;
            this.retryDeadConnections = retryDeadConnections;
            this.deadConnectionRetryTime = deadConnectionRetryTime;

            foreach (var solrConnection in connections) 
            {
                this.connectionsList.Add(new SolrConnectionWrapper(solrConnection));
            }
        }

        public SolrLoadBalancedConnection(IEnumerable<ISolrConnection> connections) : this(connections,1,0,true,0) { }

        public string Post(string relativeUrl, string s) 
        {
            return this.TryConnection(x => x.Connection.Post(relativeUrl, s));
        }

        public string PostStream(string relativeUrl, string contentType, Stream content, IEnumerable<KeyValuePair<string, string>> getParameters) 
        {
            return this.TryConnection(x => x.Connection.PostStream(relativeUrl, contentType, content, getParameters));
        }

        public string Get(string relativeUrl, IEnumerable<KeyValuePair<string, string>> parameters) 
        {
            return this.TryConnection(x => x.Connection.Get(relativeUrl, parameters));
        }

        /// <summary>
        /// Loops through each connection and tries each one a certain number of times, delaying a certain number of milliseconds between each try.
        /// Also retries dead connections if a certain time has been passed.
        /// </summary>
        /// <param name="function">Decorated function to execute.</param>
        /// <returns>Result of Solr operation as a string.</returns>
        private string TryConnection(Func<SolrConnectionWrapper, string> function) 
        {
            Exception lastException = null;

            for (int i = 0; i < this.connectionsList.Count(); i++)
            {
                for (int retry = 1; retry < this.numberOfRetries; retry++)
                {
                    if (this.connectionsList[i].IsActive || this.ShouldRecheckDeadConnection(this.connectionsList[i]))
                    {
                        try
                        {
                            var result = function(this.connectionsList[i]);
                            this.connectionsList[i].IsActive = true;
                            this.connectionsList[i].LastUsed = DateTime.Now;
                            return result;
                        }
                        catch (SolrConnectionException exception)
                        {
                            this.connectionsList[i].IsActive = false;
                            this.connectionsList[i].LastChecked = DateTime.Now;

                            lastException = exception;
                            Thread.Sleep(this.retryInterval);
                        }
                    }
                }
            }

            // TODO: .NET 4.0 : AggregateException !
            throw new SolrConnectionException("Tried all connections, no active nodes found.", lastException);    
        }

        /// <summary>
        /// Evaluates conditions for a dead connection to be re-tried.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private bool ShouldRecheckDeadConnection(SolrConnectionWrapper connection) 
        {
            return (
                !connection.IsActive &&
                this.retryDeadConnections &&
                (DateTime.Now - connection.LastChecked).Milliseconds >= this.deadConnectionRetryTime);
        }

        /// <summary>
        /// Wraps the connection so we can add some extra state data to it.
        /// </summary>
        private class SolrConnectionWrapper 
        {
            public ISolrConnection Connection { get; private set; }

            public bool IsActive { get; set; }

            public DateTime LastChecked { get; set; }

            public DateTime LastUsed { get; set; }

            public SolrConnectionWrapper(ISolrConnection connection) 
            {
                Connection = connection;
                IsActive = true;
            }
        }
    }
}