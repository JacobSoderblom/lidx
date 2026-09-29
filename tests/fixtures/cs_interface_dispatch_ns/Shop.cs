namespace Shop
{
    public interface IPublisher
    {
        void PublishDeleted(int id);
    }

    public class Publisher : Shop.IPublisher
    {
        public void PublishDeleted(int id)
        {
        }
    }

    public class Coordinator
    {
        private readonly Shop.IPublisher _pub;

        public void Delete(int id)
        {
            _pub.PublishDeleted(id);
        }
    }
}
