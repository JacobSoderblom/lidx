namespace Shop
{
    public interface IPublisher
    {
        void Publish();
    }

    public class Publisher : IPublisher
    {
        void IPublisher.Publish()
        {
        }
    }

    public class PubCaller
    {
        private readonly IPublisher _pub;

        public PubCaller(IPublisher pub)
        {
            _pub = pub;
        }

        public void Fire()
        {
            _pub.Publish();
        }
    }
}
