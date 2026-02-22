import React from 'react';

import { useClientSearch } from '../hooks/useClientSearch';
import { useAppStore } from '../store/useAppStore';
import type { ClientStatus } from '../types';

export function FilterPanel() {
  const { filters, setFilters, resetFilters, isSearching } = useAppStore();
  const { search } = useClientSearch();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    search();
  };

  return (
    <form onSubmit={handleSubmit} className="bg-white rounded-lg shadow p-6 space-y-4">
      <h2 className="text-lg font-semibold text-gray-800">Search Filters</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {/* Date From */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Date From
          </label>
          <input
            type="date"
            value={filters.date_from ?? ''}
            onChange={(e) =>
              setFilters({ date_from: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Date To */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Date To
          </label>
          <input
            type="date"
            value={filters.date_to ?? ''}
            onChange={(e) =>
              setFilters({ date_to: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Status */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Status
          </label>
          <select
            value={filters.status ?? ''}
            onChange={(e) =>
              setFilters({ status: (e.target.value as ClientStatus) || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All</option>
            <option value="active">Active</option>
            <option value="inactive">Inactive</option>
            <option value="pending">Pending</option>
          </select>
        </div>

        {/* Region */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Region
          </label>
          <input
            type="text"
            placeholder="e.g. northeast"
            value={filters.region ?? ''}
            onChange={(e) =>
              setFilters({ region: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Custom Field */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Custom Field
          </label>
          <input
            type="text"
            placeholder="Search custom field…"
            value={filters.custom_field ?? ''}
            onChange={(e) =>
              setFilters({ custom_field: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      <div className="flex gap-3 pt-2">
        <button
          type="submit"
          disabled={isSearching}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {isSearching ? 'Searching…' : 'Search'}
        </button>
        <button
          type="button"
          onClick={resetFilters}
          className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200 transition-colors"
        >
          Reset
        </button>
      </div>
    </form>
  );
}
