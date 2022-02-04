from abc import ABC, abstractmethod, abstractproperty

class CrabalertObserver(ABC):

    @abstractmethod
    async def notify_crab_item(self, infos_nft, token_id, price, timestamp_transaction):
        raise NotImplementedError()

    @abstractmethod
    async def notify_egg_item(self, infos_family_nft, infos_nft, token_id, price, timestamp_transaction):
        raise NotImplementedError()

    @abstractproperty
    async def id(self):
        raise NotImplementedError()
