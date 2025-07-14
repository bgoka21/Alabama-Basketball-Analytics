import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, LabelList
} from 'recharts';

export default function HeadToHeadNet() {
  const [rival, setRival] = useState('');
  const [data, setData] = useState([
    { name: 'Alabama', net: 0 },
    { name: 'Rival',    net: 0 }
  ]);

  useEffect(() => {
    const query = rival ? `?school=${encodeURIComponent(rival)}` : '';
    fetch(`/api/draft/net${query}`)
      .then(res => res.json())
      .then(js => {
        setData([
          { name: 'Alabama', net: js.alabama_net },
          { name: rival || 'Rival', net: js.rival_net }
        ]);
      });
  }, [rival]);

  return (
    <div className="p-6 bg-white dark:bg-gray-800 rounded-2xl shadow-lg">
      <h2 className="text-2xl font-semibold mb-4">Head-to-Head NET</h2>
      <select
        className="border rounded px-3 py-2 mb-6 w-full"
        value={rival}
        onChange={e => setRival(e.target.value)}
      >
        <option value="">Select Rival School</option>
        <option value="Kentucky">Kentucky</option>
        <option value="Duke">Duke</option>
        <option value="Gonzaga">Gonzaga</option>
        <!-- add more options or load dynamically -->
      </select>
      <BarChart width={500} height={300} data={data} layout="vertical" margin={{ left:20, right:20 }}>
        <CartesianGrid strokeDasharray="4 2" />
        <XAxis type="number" tickFormatter={v => `$${(v/1e6).toFixed(1)}M`} />
        <YAxis dataKey="name" type="category" width={100} />
        <Tooltip formatter={v => `$${v.toLocaleString()}`} />
        <Bar dataKey="net" fill="#9E1B32">
          <LabelList position="right" formatter={v => `$${(v/1e6).toFixed(1)}M`} />
        </Bar>
      </BarChart>
    </div>
  );
}
