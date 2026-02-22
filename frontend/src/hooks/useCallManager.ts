import { initiateCalls } from '../api/client';
import { useAppStore } from '../store/useAppStore';

export function useCallManager() {
  const { selectedIds, setCallStatus, setIsCalling } = useAppStore();

  const callSelected = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) return;

    setIsCalling(true);
    ids.forEach((id) => setCallStatus(id, 'calling'));

    try {
      const response = await initiateCalls(ids);
      response.results.forEach((r) => {
        setCallStatus(r.client_id, r.status === 'initiated' ? 'initiated' : 'failed');
      });
    } catch {
      ids.forEach((id) => setCallStatus(id, 'failed'));
    } finally {
      setIsCalling(false);
    }
  };

  return { callSelected };
}
