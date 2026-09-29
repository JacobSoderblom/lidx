namespace Other
{
    public interface IPublisher
    {
        void PublishDeleted(int id);
    }

    public class OtherImpl : Other.IPublisher
    {
        public void PublishDeleted(int id)
        {
        }
    }
}
