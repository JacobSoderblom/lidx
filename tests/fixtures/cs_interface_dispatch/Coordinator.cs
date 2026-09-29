namespace Shop
{
    public class Coordinator
    {
        private readonly IPublisher _pub;

        public Coordinator(IPublisher pub)
        {
            _pub = pub;
        }

        public void Delete(int id)
        {
            _pub.PublishDeleted(id);
        }
    }
}
