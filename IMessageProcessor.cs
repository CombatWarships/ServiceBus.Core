namespace ServiceBus.Core
{
	public interface IMessageProcessor
	{
		Task ProcessMessage(string message);
	}
}